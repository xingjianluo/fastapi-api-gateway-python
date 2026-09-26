"""Path-template routing with version-aware upstream selection.

Path templates use ``{name}`` parameters (e.g. ``/orders/{order_id}``).
Matching precedence is fully deterministic: at each path position a literal
segment beats a parameter segment, and ties are broken by registration order.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .config import Route
from .errors import RouteNotFoundError
from .versioning import SemVer, VersionRange, select_version

_PARAM_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}")
_SEGMENT_RE = re.compile(r"^/(\{[a-zA-Z_][a-zA-Z0-9_]*\}|[^/{}]+)(/(\{[a-zA-Z_][a-zA-Z0-9_]*\}|[^/{}]+))*$")


@dataclass(frozen=True)
class _CompiledRoute:
    route: Route
    index: int
    pattern: re.Pattern[str]
    specificity: tuple[int, ...]


@dataclass(frozen=True)
class RouteMatch:
    route: Route
    path_params: dict[str, str]
    version: SemVer
    version_str: str


class Router:
    """Matches method + path against configured :class:`Route` templates."""

    def __init__(self, routes: list[Route]) -> None:
        self._compiled: list[_CompiledRoute] = [
            self._compile(route, index) for index, route in enumerate(routes)
        ]

    @staticmethod
    def _compile(route: Route, index: int) -> _CompiledRoute:
        template = route.path
        if not template.startswith("/"):
            raise ValueError(f"route path must start with '/': {template!r}")
        normalised = template if template == "/" else template.rstrip("/")
        if _SEGMENT_RE.match(normalised) is None:
            raise ValueError(f"invalid path template: {template!r}")

        specificity: list[int] = []
        seen_params: set[str] = set()

        def replace(match: re.Match[str]) -> str:
            name = match.group(1)
            if name in seen_params:
                raise ValueError(f"duplicate path parameter {name!r} in {template!r}")
            seen_params.add(name)
            return f"(?P<{name}>[^/]+)"

        for segment in normalised.strip("/").split("/"):
            if segment.startswith("{") and segment.endswith("}"):
                specificity.append(1)  # parameter segment = lower priority
            else:
                specificity.append(0)  # literal segment = higher priority
        regex = _PARAM_RE.sub(replace, normalised)
        return _CompiledRoute(
            route=route,
            index=index,
            pattern=re.compile(f"^{regex}$"),
            specificity=tuple(specificity),
        )

    def resolve(
        self,
        method: str,
        path: str,
        requested_version: VersionRange | None,
    ) -> RouteMatch:
        normalised = path if path == "/" else path.rstrip("/") or "/"

        matches: list[tuple[_CompiledRoute, dict[str, str]]] = []
        for compiled in self._compiled:
            match = compiled.pattern.match(normalised)
            if match is not None:
                matches.append((compiled, match.groupdict()))

        if not matches:
            raise RouteNotFoundError(
                f"no route for {method} {path}",
                details={"method": method, "path": path},
            )

        # Most specific template first; registration order breaks ties.
        matches.sort(key=lambda item: (item[0].specificity, item[0].index))
        method_matches = [
            (compiled, params)
            for compiled, params in matches
            if method in compiled.route.methods
        ]
        if not method_matches:
            compiled, _ = matches[0]
            raise RouteNotFoundError(
                f"method {method} not allowed on {path}",
                details={
                    "method": method,
                    "path": path,
                    "allowed_methods": sorted(compiled.route.methods),
                },
            )

        compiled, params = method_matches[0]
        available = [SemVer.parse(v) for v in compiled.route.versions]
        chosen = select_version(available, requested_version)
        return RouteMatch(
            route=compiled.route,
            path_params=params,
            version=chosen,
            version_str=str(chosen),
        )


# HTTP methods that are safe to replay on another upstream after a failure.
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})
