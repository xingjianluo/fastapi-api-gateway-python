"""Deterministic version negotiation and rejection distinctions."""

from __future__ import annotations

import logging

import pytest

from gateway.errors import VersionNotAcceptableError
from gateway.versioning import SemVer, VersionRange, select_version

log = logging.getLogger("gateway.test.versioning")

AVAILABLE = ["1.0.0", "1.2.0", "2.0.0", "2.1.0"]


@pytest.mark.parametrize(
    "header,expected",
    [
        (None, "2.1.0"),                 # no constraint -> highest
        ("*", "2.1.0"),
        ("1", "1.2.0"),                  # major line, highest minor
        ("1.2", "1.2.0"),
        ("1.2.0", "1.2.0"),              # exact
        ("2", "2.1.0"),
        ("^1.2.0", "1.2.0"),             # caret within major
        ("~1.2.0", "1.2.0"),             # tilde within minor
        (">=1.0.0,<2.0.0", "1.2.0"),
        (">=2.1.0", "2.1.0"),
        ("v1", "1.2.0"),
    ],
)
def test_select_version_deterministic(header, expected):
    rng = VersionRange.parse(header) if header is not None else None
    chosen = select_version([SemVer.parse(v) for v in AVAILABLE], rng)
    log.info("version input header=%r range=%s -> chosen=%s (expected %s)",
             header, rng.describe() if rng else "*", chosen, expected)
    assert str(chosen) == expected


def test_selection_is_stable_across_calls():
    """Same inputs must always produce the same selected version."""
    avail = [SemVer.parse(v) for v in AVAILABLE]
    results = {
        str(select_version(avail, VersionRange.parse(">=1.0.0")))
        for _ in range(50)
    }
    assert results == {"2.1.0"}


@pytest.mark.parametrize(
    "header",
    ["3", "1.5", "^2.5", "<1.0.0", "2.2.0", ">=3.0.0"],
)
def test_version_negotiation_failure_is_distinguishable(header):
    with pytest.raises(VersionNotAcceptableError) as excinfo:
        select_version(
            [SemVer.parse(v) for v in AVAILABLE], VersionRange.parse(header)
        )
    err = excinfo.value
    log.info("reject input=%r code=%s details=%s", header, err.code.value, err.details)
    assert err.code.value == "version_not_acceptable"
    assert err.http_status == 400
    assert err.details["available"] == AVAILABLE
    assert "requested" in err.details


def test_malformed_version_header():
    with pytest.raises(ValueError):
        VersionRange.parse("not-a-version")
