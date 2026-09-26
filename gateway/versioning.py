"""Semantic-version parsing and deterministic version negotiation.

The caller expresses what it can accept via an ``X-Accept-Version`` header:

* ``1`` / ``1.2``        -> that major line, at least the given minor
* ``1.2.3``              -> exactly that version
* ``>=1.2.0,<2.0.0``     -> comma-separated comparison list
* ``^1.2.3``             -> caret: ``>=1.2.3,<2.0.0`` (zero majors handled)
* ``~1.2.3``             -> tilde: ``>=1.2.3,<1.3.0``
* absent / ``*``         -> the highest version the route offers

Selection is deterministic: among all available versions inside the accepted
range the *highest* one wins.  If none fits, ``VersionNotAcceptableError``
carries both the requested range and the offered versions.
"""

from __future__ import annotations

from dataclasses import dataclass

from .errors import VersionNotAcceptableError


@dataclass(frozen=True)
class SemVer:
    major: int
    minor: int
    patch: int

    @staticmethod
    def parse(text: str) -> "SemVer":
        raw = text.strip().lstrip("vV=")
        if not raw:
            raise ValueError("empty version")
        parts = raw.split(".")
        if len(parts) > 3 or any(not p.isdigit() for p in parts):
            raise ValueError(f"invalid semantic version: {text!r}")
        nums = [int(p) for p in parts] + [0, 0, 0]
        return SemVer(nums[0], nums[1], nums[2])

    def _key(self) -> tuple[int, int, int]:
        return (self.major, self.minor, self.patch)

    def __lt__(self, other: "SemVer") -> bool:
        return self._key() < other._key()

    def __le__(self, other: "SemVer") -> bool:
        return self._key() <= other._key()

    def __gt__(self, other: "SemVer") -> bool:
        return self._key() > other._key()

    def __ge__(self, other: "SemVer") -> bool:
        return self._key() >= other._key()

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def _bump_patch(v: SemVer) -> SemVer:
    return SemVer(v.major, v.minor, v.patch + 1)


def _bump_minor(v: SemVer) -> SemVer:
    return SemVer(v.major, v.minor + 1, 0)


def _bump_major(v: SemVer) -> SemVer:
    return SemVer(v.major + 1, 0, 0)


@dataclass(frozen=True)
class VersionRange:
    """Inclusive lower bound :attr:`ge`, exclusive upper bound :attr:`lt`."""

    ge: SemVer | None = None
    lt: SemVer | None = None

    @staticmethod
    def parse(header_value: str) -> "VersionRange":
        text = header_value.strip()
        if text in ("", "*", "x", "X"):
            return VersionRange()

        result = VersionRange()
        for raw_constraint in text.split(","):
            constraint = raw_constraint.strip()
            if not constraint:
                continue
            result = result._intersect(_parse_single_constraint(constraint))
        return result

    def contains(self, version: SemVer) -> bool:
        if self.ge is not None and version < self.ge:
            return False
        if self.lt is not None and version >= self.lt:
            return False
        return True

    def describe(self) -> str:
        parts = []
        if self.ge is not None:
            parts.append(f">={self.ge}")
        if self.lt is not None:
            parts.append(f"<{self.lt}")
        return ",".join(parts) if parts else "*"

    def _intersect(self, other: "VersionRange") -> "VersionRange":
        ge = None
        if self.ge is None:
            ge = other.ge
        elif other.ge is None:
            ge = self.ge
        else:
            ge = max(self.ge, other.ge)

        lt = None
        if self.lt is None:
            lt = other.lt
        elif other.lt is None:
            lt = self.lt
        else:
            lt = min(self.lt, other.lt)

        if ge is not None and lt is not None and ge >= lt:
            raise ValueError(f"unsatisfiable version range: {self} & {other}")
        return VersionRange(ge=ge, lt=lt)


def _parse_single_constraint(constraint: str) -> VersionRange:
    c = constraint
    for op in (">=", "<=", "==", ">", "<", "=", "^", "~"):
        if c.startswith(op):
            operand = c[len(op):].strip()
            version = SemVer.parse(operand)
            if op == ">=":
                return VersionRange(ge=version)
            if op == ">":
                return VersionRange(ge=_bump_patch(version))
            if op == "<=":
                return VersionRange(lt=_bump_patch(version))
            if op == "<":
                return VersionRange(lt=version)
            if op in ("==", "="):
                return VersionRange(ge=version, lt=_bump_patch(version))
            if op == "^":
                if version.major > 0:
                    return VersionRange(ge=version, lt=_bump_major(version))
                if version.minor > 0:
                    return VersionRange(ge=version, lt=_bump_minor(version))
                return VersionRange(ge=version, lt=_bump_patch(version))
            # op == "~"
            parts = operand.split(".")
            if len(parts) == 1:
                return VersionRange(ge=version, lt=_bump_major(version))
            return VersionRange(ge=version, lt=_bump_minor(version))

    # Bare version: "1" / "1.2" pin the major line; "1.2.3" is exact.
    version = SemVer.parse(c)
    parts = c.strip().lstrip("vV").split(".")
    if len(parts) <= 2:
        return VersionRange(ge=version, lt=_bump_major(version))
    return VersionRange(ge=version, lt=_bump_patch(version))


def select_version(
    available: list[SemVer],
    requested: VersionRange | None,
) -> SemVer:
    """Deterministically pick the highest available version in the accepted range."""
    if not available:
        raise VersionNotAcceptableError(
            "route offers no versions",
            details={"requested": requested.describe() if requested else "*",
                     "available": []},
        )
    rng = requested or VersionRange()
    candidates = sorted(v for v in available if rng.contains(v))
    if not candidates:
        raise VersionNotAcceptableError(
            f"no offered version satisfies {rng.describe()}",
            details={
                "requested": rng.describe(),
                "available": [str(v) for v in sorted(available)],
            },
        )
    return candidates[-1]
