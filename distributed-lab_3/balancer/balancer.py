"""Простий балансувальник: round-robin."""
import itertools

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

SERVERS = [
    "http://server-1:8000",
    "http://server-2:8000",
    "http://server-3:8000",
]
next_server = itertools.cycle(SERVERS)

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
client = httpx.AsyncClient(timeout=10.0)


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(request: Request, path: str):
    server = next(next_server)
    headers = {k: v for k, v in request.headers.items()
               if k.lower() not in ("host", "content-length")}
    try:
        r = await client.request(
            request.method, f"{server}/{path}",
            headers=headers, content=await request.body(), params=request.query_params,
        )
    except httpx.HTTPError as exc:
        return JSONResponse(status_code=502,
                            content={"status": "error", "message": f"{server} недоступний: {exc}"})
    skip = ("content-length", "content-encoding", "transfer-encoding", "connection")
    return Response(r.content, r.status_code,
                    {k: v for k, v in r.headers.items() if k.lower() not in skip})
