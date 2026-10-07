import logging

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.weights import pick_weighted

log = logging.getLogger(__name__)

client = TestClient(create_app())


def test_proxy_weighted_selection_is_deterministic_and_sticky():
    # 演示装配对 get-item 配置了 v1/v2 = 90/10 权重，
    # 同一调用方反复请求应稳定落到同一（加权选出的）版本
    expected = pick_weighted([(1, 90), (2, 10)], "get-item", "demo")
    for i in range(5):
        resp = client.get("/items/1", headers={"x-caller-id": "demo", "x-accept-versions": "1,2"})
        log.info("GET /items/1 accept=1,2 (round %d) -> %s %s", i, resp.status_code, resp.json())
        assert resp.status_code == 200
        assert resp.json()["version"] == expected


def test_proxy_respects_accepted_versions_under_weights():
    # 只接受 v1 时，无论权重如何都应落到 v1
    resp = client.get("/items/1", headers={"x-caller-id": "demo", "x-accept-versions": "1"})
    log.info("GET /items/1 accept=1 -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 200
    assert resp.json()["version"] == 1


def test_version_not_satisfied_maps_to_406():
    resp = client.get("/items/1", headers={"x-accept-versions": "9"})
    log.info("GET /items/1 accept=9 -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 406
    assert resp.json()["error"]["category"] == "version_not_satisfied"


def test_unknown_route_maps_to_404():
    resp = client.get("/missing")
    log.info("GET /missing -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 404
    assert resp.json()["error"]["category"] == "route_not_found"
