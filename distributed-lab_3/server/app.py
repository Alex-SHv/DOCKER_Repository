import logging
import os
import time
import uuid
from contextlib import asynccontextmanager

import pymysql
import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException as StarletteHTTPException

INSTANCE_ID = os.getenv("INSTANCE_ID", "backend-1")
MAX_DELAY_MS = 2000

STARTUP_RETRIES = int(os.getenv("STARTUP_RETRIES", "30"))
STARTUP_DELAY_SEC = float(os.getenv("STARTUP_DELAY_SEC", "2"))

# База логів цього сервера (у кожного сервера своя).
LOG_DB = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "app"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "logs"),
    "charset": "utf8mb4",
    "autocommit": True,
    "connect_timeout": 3,
}

logging.basicConfig(
    level=logging.INFO,
    format=f"[{INSTANCE_ID}] %(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("backend")

CREATE_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS request_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    instance_id VARCHAR(64) NOT NULL,
    request_id VARCHAR(64) NOT NULL,
    stage VARCHAR(40) NOT NULL,
    method VARCHAR(10),
    endpoint VARCHAR(255),
    status_code INT,
    duration_ms DOUBLE,
    error_message TEXT,
    created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_request_id (request_id),
    INDEX idx_instance_id (instance_id)
)
"""

LOG_INSERT_SQL = """
INSERT INTO request_logs
    (instance_id, request_id, stage, method, endpoint, status_code, duration_ms, error_message)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


def init_log_table():
    """Чекає на БД логів і створює таблицю. Якщо БД недоступна — сервер все одно працює."""
    for attempt in range(1, STARTUP_RETRIES + 1):
        try:
            conn = pymysql.connect(**LOG_DB)
        except pymysql.MySQLError as exc:
            log.warning("БД логів ще не готова (спроба %d/%d): %s",
                        attempt, STARTUP_RETRIES, exc)
            time.sleep(STARTUP_DELAY_SEC)
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(CREATE_LOG_TABLE)
            log.info("БД логів: таблиця готова")
        except pymysql.MySQLError as exc:
            log.warning("Не вдалося створити таблицю логів: %s", exc)
        finally:
            conn.close()
        return
    log.error("БД логів недоступна після %d спроб", STARTUP_RETRIES)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await run_in_threadpool(init_log_table)  # не блокує event loop
    yield


app = FastAPI(title=f"Backend {INSTANCE_ID}", lifespan=lifespan)


def write_log(stage, request_id, method, endpoint,
              status_code=None, duration_ms=None, error=None):
    try:
        conn = pymysql.connect(**LOG_DB)
        try:
            with conn.cursor() as cur:
                cur.execute(LOG_INSERT_SQL, (INSTANCE_ID, request_id, stage, method,
                                             endpoint, status_code, duration_ms, error))
        finally:
            conn.close()
    except Exception as exc:
        log.error("Не вдалося записати журнал (%s): %s", stage, exc)


@app.middleware("http")
async def request_lifecycle(request: Request, call_next):
    t0 = time.perf_counter()
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex
    request.state.request_id = request_id
    request.state.error = None
    method, endpoint = request.method, request.url.path
    track = endpoint != "/health"

    if track:
        await run_in_threadpool(write_log, "received", request_id, method, endpoint)

    try:
        response = await call_next(request)
    except Exception as exc:
        log.exception("Внутрішня помилка")
        request.state.error = f"Internal: {exc}"[:500]
        response = JSONResponse(
            status_code=500,
            content={"status": "error", "instance_id": INSTANCE_ID,
                     "request_id": request_id, "message": "Internal error"},
        )

    if request.state.error is None and response.status_code >= 400:
        request.state.error = f"HTTP {response.status_code}"

    if track:
        duration = round((time.perf_counter() - t0) * 1000, 2)
        await run_in_threadpool(write_log, "responded", request_id, method, endpoint,
                                response.status_code, duration, request.state.error)

    response.headers["X-Instance-ID"] = INSTANCE_ID
    response.headers["X-Request-ID"] = request_id
    return response


def error_payload(request, message):
    return {
        "status": "error",
        "instance_id": INSTANCE_ID,
        "request_id": getattr(request.state, "request_id", None),
        "message": message,
    }


# Starlette-версія покриває і HTTPException FastAPI, і 404/405 від роутера.
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    request.state.error = f"HTTP {exc.status_code}: {exc.detail}"[:500]
    return JSONResponse(status_code=exc.status_code,
                        content=error_payload(request, str(exc.detail)))


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    request.state.error = f"Validation: {exc.errors()}"[:500]
    return JSONResponse(status_code=422,
                        content=error_payload(request, "Невалідний запит"))


@app.get("/health")
def health():
    return {"status": "ok", "instance_id": INSTANCE_ID}


@app.get("/api/test-load")
@app.get("/api/work")
def test_load(request: Request, delay_ms: int = Query(100, ge=0, le=MAX_DELAY_MS)):
    write_log("processing", request.state.request_id, request.method, request.url.path)
    time.sleep(delay_ms / 1000)
    return {
        "status": "success",
        "instance_id": INSTANCE_ID,
        "request_id": request.state.request_id,
        "message": "Request processed successfully",
        "delay_ms": delay_ms,
    }


@app.get("/api/stats")
def stats():
    """Статистика цього сервера за його власним журналом."""
    try:
        conn = pymysql.connect(**LOG_DB)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*), COALESCE(SUM(status_code >= 400), 0), "
                    "ROUND(AVG(duration_ms), 2) FROM request_logs "
                    "WHERE stage='responded' AND instance_id=%s",
                    (INSTANCE_ID,),
                )
                total, errors, avg_ms = cur.fetchone()
        finally:
            conn.close()
    except pymysql.MySQLError as exc:
        log.error("Не вдалося прочитати статистику: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "error", "instance_id": INSTANCE_ID,
                     "message": "База логів недоступна"},
        )
    return {
        "instance_id": INSTANCE_ID,
        "total_requests": int(total),
        "errors": int(errors),
        "avg_response_ms": float(avg_ms) if avg_ms is not None else None,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
