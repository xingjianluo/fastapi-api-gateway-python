"""本地多上游 API 网关层。"""

from gateway.clock import Clock, ManualClock, SystemClock
from gateway.errors import ErrorCategory, GatewayError
from gateway.gateway import Gateway
from gateway.models import (
    Caller,
    GatewayRequest,
    GatewayResponse,
    Route,
    RouteVersion,
    Upstream,
)
from gateway.ratelimit import RateLimiter, RateLimitRule
from gateway.router import Router
from gateway.circuit import CircuitBreakerRegistry, CircuitConfig
from gateway.upstreams import MockUpstream

__all__ = [
    "Caller",
    "CircuitBreakerRegistry",
    "CircuitConfig",
    "Clock",
    "ErrorCategory",
    "Gateway",
    "GatewayError",
    "GatewayRequest",
    "GatewayResponse",
    "ManualClock",
    "MockUpstream",
    "RateLimiter",
    "RateLimitRule",
    "Route",
    "RouteVersion",
    "Router",
    "SystemClock",
    "Upstream",
]
