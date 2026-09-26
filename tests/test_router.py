"""Path-template precedence, path params and version-aware route resolution."""

from __future__ import annotations

import logging

import pytest

from gateway.config import Route, UpstreamConfig
from gateway.errors import RouteNotFoundError, VersionNotAcceptableError
from gateway.router import Router
from gateway.versioning import VersionRange

log = logging.getLogger("gateway.test.router")


def route(path, methods=("GET",), versions=("1.0.0",)):
    return Route(
        path=path,
        methods=frozenset(methods),
        versions=versions,
        upstreams=(UpstreamConfig(name=path.strip("/").replace("/", "-") or "root"),),
    )


def test_literal_beats_parameter():
    router = Router([route("/orders/{id}"), route("/orders/special")])
    m = router.resolve("GET", "/orders/special", None)
    log.info("/orders/special -> %s (literal wins)", m.route.path)
    assert m.route.path == "/orders/special"

    m = router.resolve("GET", "/orders/77", None)
    log.info("/orders/77 -> %s params=%s", m.route.path, m.path_params)
    assert m.route.path == "/orders/{id}"
    assert m.path_params == {"id": "77"}


def test_registration_order_breaks_ties():
    router = Router([route("/a/{x}"), route("/a/{y}")])
    m = router.resolve("GET", "/a/1", None)
    assert m.route.path == "/a/{x}"


def test_trailing_slash_normalised():
    router = Router([route("/items")])
    assert router.resolve("GET", "/items/", None).route.path == "/items"


def test_unknown_path_and_bad_method_distinct():
    router = Router([route("/items", methods=("POST",))])
    with pytest.raises(RouteNotFoundError) as exc:
        router.resolve("GET", "/missing", None)
    assert exc.value.code.value == "route_not_found"
    log.info("missing path -> %s", exc.value.details)

    with pytest.raises(RouteNotFoundError) as exc:
        router.resolve("GET", "/items", None)
    log.info("bad method -> %s allowed=%s",
             exc.value.code.value, exc.value.details["allowed_methods"])
    assert "POST" in exc.value.details["allowed_methods"]


def test_version_chosen_per_route():
    router = Router([route("/v", versions=("1.0.0", "2.0.0"))])
    m = router.resolve("GET", "/v", VersionRange.parse("1"))
    assert m.version_str == "1.0.0"
    m = router.resolve("GET", "/v", None)
    assert m.version_str == "2.0.0"
    with pytest.raises(VersionNotAcceptableError):
        router.resolve("GET", "/v", VersionRange.parse("3"))


def test_invalid_template_rejected_at_build():
    with pytest.raises(ValueError):
        Router([route("no-leading-slash")])
    with pytest.raises(ValueError):
        Router([route("/dup/{x}/{x}")])
