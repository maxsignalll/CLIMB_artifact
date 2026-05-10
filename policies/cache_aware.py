from typing import Dict, Optional, Set

from .base import PolicyBase


class CacheAwarePolicy(PolicyBase):
    name = "cache_aware"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.active_set: Set[str] = set()
        self.lru: list[str] = []
        self.current_adapter: Optional[str] = None
        self.cluster_left: int = 0

    def update_active_sets(self, now: float) -> None:
        sched = self.scheduler
        for adapter_id in sched.inflight.keys():
            if adapter_id not in self.active_set:
                self._activate(adapter_id)
        sched.current_adapter = self.current_adapter
        sched.cluster_left = self.cluster_left

    def pick_next_adapter(self, now: float) -> Optional[str]:
        sched = self.scheduler
        ready_active = [a for a in self.active_set if sched.queue_len(a) > 0]
        if self.current_adapter in ready_active and self.cluster_left > 0:
            return self.current_adapter
        if ready_active:
            adapter_id = self._pick_least_recent(ready_active)
            self._set_current(adapter_id)
            return adapter_id
        miss = self._pick_miss_adapter(now)
        if miss is None:
            return None
        if not self._admit(miss):
            return None
        self._set_current(miss)
        return miss

    def on_dispatch(self, req) -> None:
        self._touch(req.adapter_id)
        if self.current_adapter == req.adapter_id and self.cluster_left > 0:
            self.cluster_left -= 1
        self.scheduler.current_adapter = self.current_adapter
        self.scheduler.cluster_left = self.cluster_left

    def snapshot(self, now: float) -> Dict[str, Optional[float]]:
        return {"cache_mode": "lru"}

    def lru_ranks(self) -> Dict[str, int]:
        return {adapter_id: idx for idx, adapter_id in enumerate(self.lru)}

    def _pick_least_recent(self, candidates: list[str]) -> str:
        rank = self.lru_ranks()
        return min(candidates, key=lambda a: rank.get(a, 0))

    def _pick_miss_adapter(self, now: float) -> Optional[str]:
        sched = self.scheduler
        best_adapter = None
        best_age = None
        for adapter_id, queue in sched.queues.items():
            if not queue:
                continue
            if adapter_id in self.active_set:
                continue
            age = now - queue[0].arrival_ts
            if best_age is None or age > best_age:
                best_age = age
                best_adapter = adapter_id
        return best_adapter

    def _admit(self, adapter_id: str) -> bool:
        if adapter_id in self.active_set:
            return True
        if len(self.active_set) < self.k:
            self._activate(adapter_id)
            return True
        evict = self._find_evict_candidate()
        if evict is None:
            return False
        self._evict(evict)
        self._activate(adapter_id)
        return True

    def _find_evict_candidate(self) -> Optional[str]:
        sched = self.scheduler
        for adapter_id in list(self.lru):
            if adapter_id == self.current_adapter and sched.queue_len(adapter_id) > 0:
                continue
            if sched.inflight_count(adapter_id) == 0:
                return adapter_id
        return None

    def _activate(self, adapter_id: str) -> None:
        if adapter_id in self.active_set:
            return
        self.active_set.add(adapter_id)
        self.scheduler.activate(adapter_id)
        self.lru.append(adapter_id)

    def _evict(self, adapter_id: str) -> None:
        if adapter_id not in self.active_set:
            return
        if adapter_id in self.lru:
            self.lru.remove(adapter_id)
        self.active_set.remove(adapter_id)
        self.scheduler.deactivate(adapter_id)
        if self.current_adapter == adapter_id:
            self.current_adapter = None
            self.cluster_left = 0

    def _touch(self, adapter_id: str) -> None:
        if adapter_id in self.lru:
            self.lru.remove(adapter_id)
        self.lru.append(adapter_id)

    def _set_current(self, adapter_id: str) -> None:
        if self.current_adapter != adapter_id:
            self.current_adapter = adapter_id
            self.cluster_left = max(1, self.cluster_q)
