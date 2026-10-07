"""按调用方粘性的加权版本分流。

- 每个 (路由, 版本) 一个非负整数权重，新请求按权重比例落到版本上；
- 同一调用方在权重不变时稳定落到同一版本（基于稳定哈希分桶，不随机）；
- 权重快照式整体替换，替换后对新请求立刻生效，在途请求按进入时的判定走完；
- 权重为 0 的版本不接新请求。
"""

from __future__ import annotations

import hashlib
import threading
from types import MappingProxyType


class WeightManager:
    """每路由每版本的权重表，快照式整体替换（读方只见旧快照或新快照）。"""

    def __init__(self, weights: dict[tuple[str, int], int] | None = None) -> None:
        self._lock = threading.Lock()
        self._weights: MappingProxyType[tuple[str, int], int] = MappingProxyType({})
        if weights:
            self.update_weights(weights)

    def update_weights(self, weights: dict[tuple[str, int], int]) -> None:
        """原子替换全部权重（快照语义）。权重必须为非负整数。"""
        for (route_id, version), w in weights.items():
            if not isinstance(w, int) or w < 0:
                raise ValueError(
                    f"weight for ({route_id!r}, {version}) must be a non-negative int, got {w!r}"
                )
        snapshot = MappingProxyType(dict(weights))
        with self._lock:
            self._weights = snapshot

    def snapshot(self) -> MappingProxyType[tuple[str, int], int]:
        """当前权重快照（不可变）。"""
        with self._lock:
            return self._weights


def stable_bucket(route_id: str, caller_id: str, total: int) -> int:
    """把 (路由, 调用方) 确定性地映射到 [0, total) 的桶号。

    使用 sha256（不受 Python 字符串哈希随机化影响），同一输入跨进程/跨次运行结果一致。
    """
    if total <= 0:
        raise ValueError("total must be positive")
    digest = hashlib.sha256(f"{route_id} {caller_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % total


def pick_weighted(
    candidates: list[tuple[int, int]], route_id: str, caller_id: str
) -> int:
    """在 (版本号, 权重) 候选中按权重确定性选择一个版本号。

    候选按版本号升序排列后累计权重分桶；同一 (路由, 调用方, 权重分布) 输入
    永远得到同一版本。权重为 0 的候选不会被选中。候选权重全为 0 时抛 ValueError。
    """
    ordered = sorted(candidates)
    total = sum(w for _, w in ordered)
    if total <= 0:
        raise ValueError(f"all candidate weights are zero for route '{route_id}'")
    bucket = stable_bucket(route_id, caller_id, total)
    cumulative = 0
    for version, weight in ordered:
        cumulative += weight
        if bucket < cumulative:
            return version
    raise AssertionError("unreachable: bucket must fall into some candidate")
