"""加权分流：确定性、调用方粘性、运行期调整、0 权重摘流。"""

import logging
from collections import Counter

import pytest

from gateway.weights import WeightManager, pick_weighted, stable_bucket

log = logging.getLogger(__name__)

CANDIDATES = [(1, 10), (2, 90)]


def test_same_caller_always_lands_on_same_version():
    picks = {pick_weighted(CANDIDATES, "get-item", "caller-7") for _ in range(200)}
    log.info("caller-7 picked %s across 200 evaluations", picks)
    assert len(picks) == 1


def test_selection_is_deterministic_across_callers():
    first = {f"c{i}": pick_weighted(CANDIDATES, "get-item", f"c{i}") for i in range(500)}
    second = {f"c{i}": pick_weighted(CANDIDATES, "get-item", f"c{i}") for i in range(500)}
    drift = [c for c in first if first[c] != second[c]]
    log.info("500 callers evaluated twice, drift=%d", len(drift))
    assert first == second


def test_distribution_follows_weights():
    counts = Counter(pick_weighted(CANDIDATES, "get-item", f"c{i}") for i in range(20000))
    ratio = counts[2] / (counts[1] + counts[2])
    log.info("weights 10/90 over 20000 callers -> %s, v2 ratio=%.3f", dict(counts), ratio)
    assert 0.87 < ratio < 0.93


def test_zero_weight_version_gets_no_traffic():
    picks = {pick_weighted([(1, 0), (2, 5)], "get-item", f"c{i}") for i in range(1000)}
    log.info("v1 weight=0 -> picked versions %s", picks)
    assert picks == {2}


def test_all_zero_weights_rejected():
    with pytest.raises(ValueError):
        pick_weighted([(1, 0), (2, 0)], "get-item", "c1")
    log.info("all-zero weights -> ValueError")


def test_bucket_stable_regardless_of_python_hash_seed():
    # sha256 驱动，不受 PYTHONHASHSEED 影响：同一输入永远同一桶
    buckets = {stable_bucket("get-item", "caller-9", 97) for _ in range(100)}
    log.info("stable_bucket caller-9 -> %s", buckets)
    assert len(buckets) == 1


def test_weight_manager_snapshot_replacement_is_atomic():
    wm = WeightManager({("r", 1): 50, ("r", 2): 50})
    before = wm.snapshot()
    wm.update_weights({("r", 1): 0, ("r", 2): 100})
    after = wm.snapshot()
    log.info("before=%s after=%s", dict(before), dict(after))
    assert dict(before) == {("r", 1): 50, ("r", 2): 50}  # 旧快照不被改写
    assert dict(after) == {("r", 1): 0, ("r", 2): 100}
    with pytest.raises(TypeError):
        before[("r", 1)] = 1  # 快照不可变


def test_weight_update_changes_new_selections_only():
    wm = WeightManager({("r", 1): 100, ("r", 2): 0})
    snap1 = wm.snapshot()
    picks1 = {pick_weighted([(1, snap1[("r", 1)]), (2, snap1[("r", 2)])], "r", f"c{i}")
              for i in range(100)}
    wm.update_weights({("r", 1): 0, ("r", 2): 100})
    snap2 = wm.snapshot()
    picks2 = {pick_weighted([(1, snap2[("r", 1)]), (2, snap2[("r", 2)])], "r", f"c{i}")
              for i in range(100)}
    log.info("before update all -> %s; after update all -> %s", picks1, picks2)
    assert picks1 == {1} and picks2 == {2}


def test_negative_weight_rejected():
    wm = WeightManager()
    with pytest.raises(ValueError):
        wm.update_weights({("r", 1): -1})
    log.info("negative weight -> ValueError")
