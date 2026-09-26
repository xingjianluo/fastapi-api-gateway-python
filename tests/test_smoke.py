"""Smoke test that the package imports (skeleton-stage)."""


def test_package_imports() -> None:
    import gateway  # noqa: F401
    from gateway import app, breaker, clock, config, errors  # noqa: F401
    from gateway import gateway as gw_mod  # noqa: F401
    from gateway import ratelimiter, router, upstream, versioning  # noqa: F401
