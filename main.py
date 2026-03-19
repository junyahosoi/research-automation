"""
FastAPI メインアプリ
CSVアップロード・SSEストリーミング・結果ダウンロードを提供する
"""

import asyncio
import csv
import io
import json
from pathlib import Path

from fastapi import FastAPI, Request, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from processor import parse_and_deduplicate, run_pipeline
from progress_store import clear_progress, load_progress

app = FastAPI(title="ブランドリサーチツール")

# シングルユーザー前提のインメモリセッション
_session: dict = {
    "brands_data": [],  # 重複除去済みブランド行リスト
    "queue": None,       # asyncio.Queue（SSEイベントキュー）
    "task": None,        # バックグラウンドasyncio.Task
    "results": [],       # 蓄積済み結果リスト
}

OUTPUT_COLUMNS = [
    "ブランド名",
    "会社名",
    "HP URL",
    "電話番号",
    "FAX番号",
    "メールアドレス",
    "フォームURL",
    "フラグ",
]


# ---------------------------------------------------------------------------
# API エンドポイント
# ---------------------------------------------------------------------------


@app.post("/upload")
async def upload_csv(file: UploadFile):
    """CSVをアップロードして重複除去し、件数を返す。"""
    try:
        content = await file.read()
        rows, original_count = parse_and_deduplicate(content)
        _session["brands_data"] = rows
        _session["results"] = []
        return {
            "original_count": original_count,
            "deduplicated_count": len(rows),
            "message": f"{original_count}件 → {len(rows)}件に重複除去",
        }
    except ValueError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(
            status_code=500, content={"error": f"ファイルの読み込みに失敗しました: {e}"}
        )


@app.get("/resume-check")
async def resume_check():
    """前回の未完了進捗があるかチェックする。"""
    progress = await load_progress()
    if not progress or progress.get("completed"):
        return {"has_resume": False}

    processed = len(progress.get("processed_brands", []))
    total = progress.get("total_brands", 0)
    remaining = total - processed
    return {
        "has_resume": True,
        "processed": processed,
        "total": total,
        "remaining": remaining,
        "message": f"前回の続きから再開できます（{processed}/{total}件処理済、残り{remaining}件）",
    }


@app.post("/start")
async def start_processing(request: Request):
    """処理を開始する（resume=true で前回の続きから）。"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "リクエスト形式が不正です"})

    api_key: str = body.get("api_key", "").strip()
    cx: str = body.get("cx", "").strip()
    resume: bool = body.get("resume", False)

    if not api_key:
        return JSONResponse(
            status_code=400,
            content={"error": "APIキーは必須です"},
        )

    resume_from: set[str] = set()
    brands_data: list[dict] = _session["brands_data"]
    results: list[dict] = []

    if resume:
        progress = await load_progress()
        if progress:
            resume_from = set(progress.get("processed_brands", []))
            results = progress.get("results", [])
            # セッションにブランドデータがない場合（ページリロード後）は復元
            if not brands_data and progress.get("all_brands_data"):
                brands_data = progress["all_brands_data"]
                _session["brands_data"] = brands_data
    else:
        await clear_progress()

    _session["results"] = results

    # 既存タスクをキャンセル
    existing_task = _session.get("task")
    if existing_task and not existing_task.done():
        existing_task.cancel()
        try:
            await existing_task
        except asyncio.CancelledError:
            pass

    queue: asyncio.Queue = asyncio.Queue()
    _session["queue"] = queue

    async def pipeline_task():
        try:
            async for event in run_pipeline(
                brands_data, api_key, cx, resume_from, results
            ):
                await queue.put(event)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            await queue.put({"type": "error", "message": str(e)})
        finally:
            await queue.put(None)  # 終端マーカー

    _session["task"] = asyncio.create_task(pipeline_task())
    return {"status": "started"}


@app.get("/stream")
async def stream_events():
    """SSEエンドポイント：処理イベントをリアルタイムで配信する。"""

    async def event_generator():
        queue: asyncio.Queue | None = _session.get("queue")
        if queue is None:
            yield 'data: {"type": "error", "message": "処理が開始されていません"}\n\n'
            return

        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=25.0)
            except asyncio.TimeoutError:
                # keep-alive ping（Renderのプロキシタイムアウト防止）
                yield 'data: {"type": "ping"}\n\n'
                continue

            if event is None:
                yield 'data: {"type": "stream_end"}\n\n'
                break

            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Nginxのバッファリングを無効化
        },
    )


@app.get("/download")
async def download_csv():
    """処理結果をUTF-8 BOM付きCSVとしてダウンロードする。"""
    results: list[dict] = _session.get("results", [])

    # セッションに結果がなければprogress.jsonから復元
    if not results:
        progress = await load_progress()
        if progress:
            results = progress.get("results", [])

    if not results:
        return JSONResponse(status_code=404, content={"error": "ダウンロードできる結果がありません"})

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=OUTPUT_COLUMNS,
        extrasaction="ignore",
        lineterminator="\r\n",
    )
    writer.writeheader()
    writer.writerows(results)

    # BOM付きUTF-8（Excelで文字化けしない）
    csv_bytes = ("\ufeff" + output.getvalue()).encode("utf-8")

    return StreamingResponse(
        io.BytesIO(csv_bytes),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": "attachment; filename*=UTF-8''brand_research.csv"
        },
    )


@app.post("/clear")
async def clear_session():
    """セッションデータとprogress.jsonを削除する。"""
    _session["brands_data"] = []
    _session["results"] = []
    _session["queue"] = None

    task = _session.get("task")
    if task and not task.done():
        task.cancel()
    _session["task"] = None

    await clear_progress()
    return {"status": "cleared"}


@app.get("/status")
async def get_status():
    """現在のセッション状態を返す（デバッグ用）。"""
    task = _session.get("task")
    return {
        "brands_loaded": len(_session["brands_data"]),
        "results_count": len(_session["results"]),
        "task_running": task is not None and not task.done(),
    }


# ---------------------------------------------------------------------------
# 静的ファイル（フロントエンド）を最後にマウント
# ---------------------------------------------------------------------------
static_dir = Path("static")
static_dir.mkdir(exist_ok=True)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
