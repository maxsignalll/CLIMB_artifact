from typing import List, Optional, Set

from ingress.policies.base import PolicyBase


class NoSwitchPolicy(PolicyBase):
    name = "no_switch"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.locked = False

    def update_active_sets(self, now: float) -> None:
        sched = self.scheduler
        for adapter_id in list(sched.inflight.keys()):
            sched.activate(adapter_id)

        if self.locked:
            if self._active_has_work():
                return
            for adapter_id in list(sched.active_vip):
                if sched.inflight_count(adapter_id) == 0 and sched.queue_len(adapter_id) == 0:
                    sched.deactivate(adapter_id)
            for adapter_id in list(sched.active_bg):
                if sched.inflight_count(adapter_id) == 0 and sched.queue_len(adapter_id) == 0:
                    sched.deactivate(adapter_id)
            if sched.resident_set():
                return
            self.locked = False

        vip_backlog, bg_backlog = self._backlog_by_class()
        for adapter_id in self._ranked_candidates(vip_backlog, now):
            if len(sched.resident_set()) >= self.k:
                break
            if adapter_id in sched.resident_set():
                continue
            sched.activate(adapter_id)

        remaining = self.k - len(sched.active_vip)
        target_bg = min(self.bg_cap, max(0, remaining))
        for adapter_id in self._ranked_candidates(bg_backlog, now):
            if len(sched.active_bg) >= target_bg or len(sched.resident_set()) >= self.k:
                break
            if adapter_id in sched.resident_set():
                continue
            sched.activate(adapter_id)

        if len(sched.resident_set()) >= self.k:
            self.locked = True

    def pick_next_adapter(self, now: float) -> Optional[str]:
        best_adapter = None
        best_ts = None
        eligible = self.scheduler.active_vip.union(self.scheduler.active_bg)
        for adapter_id in eligible:
            queue = self.scheduler.queues.get(adapter_id)
            if not queue:
                continue
            ts = queue[0].arrival_ts
            if best_ts is None or ts < best_ts:
                best_ts = ts
                best_adapter = adapter_id
        return best_adapter

    def snapshot(self, now: float):
        return {"locked": self.locked}

    def _active_has_work(self) -> bool:
        sched = self.scheduler
        for adapter_id in sched.active_vip.union(sched.active_bg):
            if sched.queue_len(adapter_id) > 0 or sched.inflight_count(adapter_id) > 0:
                return True
        return False

    def _backlog_by_class(self) -> tuple[Set[str], Set[str]]:
        sched = self.scheduler
        vip_backlog = set()
        bg_backlog = set()
        for adapter_id in sched.adapters_with_backlog_or_inflight():
            if sched.adapter_class.get(adapter_id) == "VIP":
                vip_backlog.add(adapter_id)
            else:
                bg_backlog.add(adapter_id)
        return vip_backlog, bg_backlog

    def _ranked_candidates(self, backlog: Set[str], now: float) -> List[str]:
        sched = self.scheduler
        candidates = [a for a in backlog if sched.queue_len(a) > 0]
        def key(adapter_id: str) -> tuple:
            queue = sched.queues.get(adapter_id)
            if queue:
                ts = queue[0].arrival_ts
            else:
                ts = sched.last_arrival_ts.get(adapter_id, now)
            return (ts, adapter_id)
        return sorted(candidates, key=key)
