import logging

from fastapi.testclient import TestClient

from gateway.app import create_app

log = logging.getLogger(__name__)

client = TestClient(create_app())


def test_proxy_routes_to_highest_acceptable_version():
    resp = client.get("/items/1", headers={"x-caller-id": "demo", "x-accept-versions": "1,2"})
    log.info("GET /items/1 accept=1,2 -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 200
    assert resp.json()["version"] == 2


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


def test_admin_weights_endpoint_updates_at_runtime():
    # 默认未配置权重：走最高可接受版本
    resp = client.get("/items/1", headers={"x-caller-id": "demo", "x-accept-versions": "1,2"})
    assert resp.json()["version"] == 2
    # 运行期把 v2 权重压到 0：同一调用方立即落回 v1
    resp = client.put("/-/admin/weights/get-item", json={"weights": {"1": 100, "2": 0}})
    log.info("PUT /-/admin/weights/get-item -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 200
    resp = client.get("/items/1", headers={"x-caller-id": "demo", "x-accept-versions": "1,2"})
    log.info("after weight update -> version=%d", resp.json()["version"])
    assert resp.json()["version"] == 1
    # 恢复默认（不影响其他用例）
    client.put("/-/admin/weights/get-item", json={"weights": {"1": 0, "2": 100}})


def test_admin_capacity_endpoint_updates_rules():
    resp = client.put(
        "/-/admin/capacity/items-v2a",
        json={"capacity": 50, "reserved": {"vip": 10}},
    )
    log.info("PUT /-/admin/capacity/items-v2a -> %s %s", resp.status_code, resp.json())
    assert resp.status_code == 200
    assert resp.json()["updated"] is True
