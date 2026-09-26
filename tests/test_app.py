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
