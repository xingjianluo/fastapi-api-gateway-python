"""FastAPI HTTP layer exposing the gateway over HTTP.

Headers understood by the gateway:

* ``X-Caller-Id``      — identity used for per-caller rate limits / quotas
* ``X-Accept-Version`` — accepted API version (see :mod:`gateway.versioning`)
* ``X-Request-Cost``   — optional weight of the request for the token bucket
"""

from __future__ import annotations

import json
import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .clock import SystemClock
from .config import GatewayConfig
from .gateway import Gateway, GatewayRequest

logger = logging.getLogger("gateway.http")

# Paths that are served locally and never proxied.
_SKIP_PREFIXES = ("/health", "/_health", "/docs", "/openapi.json", "/redoc", "/")


def create_app(config: GatewayConfig) -> FastAPI:
    gateway = Gateway(config, clock=SystemClock())
    app = FastAPI(title="Multi-Upstream API Gateway", version="0.1.0")
    app.state.gateway = gateway

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def proxy(full_path: str, request: Request) -> JSONResponse:
        body = await request.body()
        try:
            cost = float(request.headers.get("x-request-cost", "1.0"))
        except ValueError:
            cost = 1.0

        gateway_request = GatewayRequest(
            method=request.method,
            path="/" + full_path,
            caller=request.headers.get("x-caller-id", "anonymous"),
            version_header=request.headers.get("x-accept-version"),
            body=body,
            cost=cost,
        )
        result = gateway.handle(gateway_request)

        payload = result.body
        headers = {
            "X-Served-By": result.upstream or "-",
            "X-Api-Version": result.version or "-",
            "X-Retried": "1" if result.retried else "0",
        }
        if result.error_code:
            headers["X-Error-Code"] = result.error_code
        if isinstance(payload, (dict, list)):
            return JSONResponse(
                status_code=result.status, content=payload, headers=headers
            )
        return JSONResponse(
            status_code=result.status,
            content={"data": payload},
            headers=headers,
        )

    return app
