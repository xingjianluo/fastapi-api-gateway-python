"""网关统一错误分类：底层异常不得直接透出。"""

from __future__ import annotations

import enum


class ErrorCategory(enum.Enum):
    ROUTE_NOT_FOUND = "route_not_found"
    VERSION_NOT_SATISFIED = "version_not_satisfied"
    RATE_LIMITED = "rate_limited"
    QUOTA_EXHAUSTED = "quota_exhausted"
    UPSTREAM_CAPACITY_EXHAUSTED = "upstream_capacity_exhausted"
    CIRCUIT_OPEN = "circuit_open"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_UNAVAILABLE = "upstream_unavailable"
    UPSTREAM_ERROR = "upstream_error"


class GatewayError(Exception):
    """所有网关对外错误的基类，携带可区分的类别。"""

    def __init__(self, category: ErrorCategory, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.category = category
        self.message = message
        self.retryable = retryable

    def to_dict(self) -> dict:
        return {
            "error": {
                "category": self.category.value,
                "message": self.message,
                "retryable": self.retryable,
            }
        }
