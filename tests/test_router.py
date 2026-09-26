import logging

import pytest

from gateway.errors import ErrorCategory, GatewayError
from gateway.models import Caller, GatewayRequest, Method, Route, RouteVersion, Upstream
from gateway.router import Router

log = logging.getLogger(__name__)


def make_router() -> Router:
    return Router(
        [
            Route(
                route_id="get-item",
                method=Method.GET,
                path_template="/items/{item_id}",
                versions=(
                    RouteVersion(1, (Upstream("items-v1-a"),)),
                    RouteVersion(2, (Upstream("items-v2-a"), Upstream("items-v2-b"))),
                ),
            ),
            Route(
                route_id="list-items",
                method=Method.GET,
                path_template="/items",
                versions=(RouteVersion(1, (Upstream("items-v1-a"),)),),
            ),
        ]
    )


def test_match_extracts_path_params():
    router = make_router()
    req = GatewayRequest(Caller("c1"), Method.GET, "/items/42")
    route, params = router.match(req)
    log.info("input path=%s -> route=%s params=%s", req.path, route.route_id, params)
    assert route.route_id == "get-item"
    assert params == {"item_id": "42"}


def test_match_unknown_path_raises_route_not_found():
    router = make_router()
    req = GatewayRequest(Caller("c1"), Method.GET, "/nope")
    with pytest.raises(GatewayError) as exc:
        router.match(req)
    log.info("input path=%s -> category=%s", req.path, exc.value.category)
    assert exc.value.category is ErrorCategory.ROUTE_NOT_FOUND


def test_version_negotiation_picks_highest_acceptable():
    router = make_router()
    route, _ = router.match(GatewayRequest(Caller("c1"), Method.GET, "/items/1"))
    caller = Caller("c1", accepted_versions=frozenset({1, 2}))
    chosen = router.select_version(route, caller)
    log.info("accepted=%s offered=[1,2] -> chosen=%d", sorted(caller.accepted_versions), chosen.version)
    assert chosen.version == 2


def test_version_negotiation_pins_to_accepted_lower_version():
    router = make_router()
    route, _ = router.match(GatewayRequest(Caller("c1"), Method.GET, "/items/1"))
    caller = Caller("c1", accepted_versions=frozenset({1}))
    chosen = router.select_version(route, caller)
    log.info("accepted=[1] offered=[1,2] -> chosen=%d", chosen.version)
    assert chosen.version == 1


def test_version_negotiation_failure_is_distinct_error():
    router = make_router()
    route, _ = router.match(GatewayRequest(Caller("c1"), Method.GET, "/items/1"))
    caller = Caller("c1", accepted_versions=frozenset({3}))
    with pytest.raises(GatewayError) as exc:
        router.select_version(route, caller)
    log.info("accepted=[3] offered=[1,2] -> category=%s msg=%s", exc.value.category, exc.value.message)
    assert exc.value.category is ErrorCategory.VERSION_NOT_SATISFIED
