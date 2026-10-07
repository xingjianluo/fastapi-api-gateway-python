"""同一路由多版本的按权重分流：调用方粘性由确定性哈希保证。

- 权重按路由整体快照替换，运行期可调，新请求立即按新快照判定；
- 同一 (路由, 调用方) 在权重不变时稳定落到同一版本（哈希分桶，不随机）；
- 权重为 0 的版本不接新请求；已在途的请求按进入时的判定走完。
"""

from __future__ import annotations

import hashlib
import logging
import threading
from types import MappingProxyType
from typing import Mapping, Sequence

from gateway.models import RouteVersion

log = logging.getLogger(__name__)


def _sticky_fraction(route_id: str, caller_id: str) -> float:
    """确定性哈希到 [0,1)：同一输入永远得到同一结果，跨进程/跨次运行可复现。"""
    digest = hashlib.sha256(f"{route_id}|{caller_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


class VersionWeighter:
    """按路由保存版本权重快照，并提供确定性的粘性选择。

    选择算法：把 (route_id, caller_id) 哈希到 [0,1) 的固定点，再按版本号升序
    累加有效权重做分桶。权重快照不变时同一调用方必落同一版本；权重变化后
    新请求按新快照重新分桶，已放行的请求不受影响（决策在进入时完成）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._weights: dict[str, MappingProxyType[int, int]] = {}

    def update_weights(self, route_id: str, weights: Mapping[int, int]) -> None:
        """原子替换某条路由的权重表（快照语义）。权重为 0 的版本不接新请求。"""
        if any(w < 0 for w in weights.values()):
            raise ValueError("weights must be >= 0")
        snapshot = MappingProxyType(dict(weights))
        with self._lock:
            self._weights[route_id] = snapshot
        log.info("weights updated route=%s weights=%s", route_id, dict(snapshot))

    def configured(self, route_id: str) -> Mapping[int, int] | None:
        """返回该路由当前权重快照；未配置返回 None（调用方走默认最高版本逻辑）。"""
        with self._lock:
            return self._weights.get(route_id)

    def select(
        self,
        route_id: str,
        caller_id: str,
        candidates: Sequence[RouteVersion],
        effective_weights: Mapping[int, int],
    ) -> RouteVersion | None:
        """在候选版本中按有效权重做粘性选择；全部权重为 0 时返回 None。

        effective_weights 由调用方给出（配置权重经灰度联动调整后的值），
        本方法只做确定性的哈希分桶，不读取任何可变状态。
        """
        weighted = sorted(
            (
                (v, effective_weights.get(v.version, 0))
                for v in candidates
                if effective_weights.get(v.version, 0) > 0
            ),
            key=lambda item: item[0].version,
        )
        total = sum(w for _, w in weighted)
        if total <= 0:
            log.info(
                "no weighted version route=%s caller=%s effective=%s",
                route_id, caller_id, dict(effective_weights),
            )
            return None
        point = _sticky_fraction(route_id, caller_id) * total
        cumulative = 0.0
        for version, weight in weighted:
            cumulative += weight
            if point < cumulative:
                log.info(
                    "sticky select route=%s caller=%s version=%d "
                    "(point=%.4f total=%d effective=%s)",
                    route_id, caller_id, version.version,
                    point, total, dict(effective_weights),
                )
                return version
        # 浮点边界兜底：落到最后一个有权重的版本
        return weighted[-1][0]
