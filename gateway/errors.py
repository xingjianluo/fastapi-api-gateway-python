"""Normalised error types for the gateway.

Bottom-level transport exceptions never leak to callers; every failure is
mapped to one of these :class:`GatewayError` subclasses.
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    ROUTE_NOT_FOUND = "route_not_found"
    VERSION_NOT_ACCEPTABLE = "version_not_acceptable"
    RATE_LIMITED = "rate_limited"              # burst over token-refill rate
    SCOPE_INSUFFICIENT = "scope_insufficient"  # single request larger than burst capacity
    QUOTA_EXHAUSTED = "quota_exhausted"        # longer-term quota used up
    UPSTREAM_CIRCUIT_OPEN = "upstream_circuit_open"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    UPSTREAM_BUSINESS_ERROR = "upstream_business_error"
    INTERNAL_ERROR = "internal_error"


class GatewayError(Exception):
    """Base class for every normalised gateway failure."""

    code: ErrorCode = ErrorCode.INTERNAL_ERROR
    http_status: int = 500

    def __init__(self, message: str = "", *, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {
            "error": {
                "code": self.code.value,
                "message": self.message or self.code.value,
                "details": self.details,
            }
        }


class RoutingError(GatewayError):
    pass


class RouteNotFoundError(RoutingError):
    code = ErrorCode.ROUTE_NOT_FOUND
    http_status = 404


class VersionNotAcceptableError(RoutingError):
    code = ErrorCode.VERSION_NOT_ACCEPTABLE
    http_status = 400


class RateLimitError(GatewayError):
    pass


class RateLimitedError(RateLimitError):
    code = ErrorCode.RATE_LIMITED
    http_status = 429


class ScopeInsufficientError(RateLimitError):
    code = ErrorCode.SCOPE_INSUFFICIENT
    http_status = 429


class QuotaExhaustedError(RateLimitError):
    code = ErrorCode.QUOTA_EXHAUSTED
    http_status = 429


class UpstreamError(GatewayError):
    pass


class UpstreamTimeoutError(UpstreamError):
    code = ErrorCode.UPSTREAM_TIMEOUT
    http_status = 504


class UpstreamUnavailableError(UpstreamError):
    code = ErrorCode.UPSTREAM_UNAVAILABLE
    http_status = 502


class UpstreamBusinessError(UpstreamError):
    code = ErrorCode.UPSTREAM_BUSINESS_ERROR
    http_status = 502


class CircuitOpenError(UpstreamError):
    code = ErrorCode.UPSTREAM_CIRCUIT_OPEN
    http_status = 503
