from typing import Optional, Set

from .base import PolicyBase


class GateRRPolicy(PolicyBase):
    name = "gate_rr"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.vip_rr_idx = 0
        self.bg_rr_idx = 0
        self.bg_paused = False

    def update_active_sets(self, now: float) -> None:
        sched = self.scheduler
        self.bg_paused = False
        for adapter_id in list(sched.active_vip):
            if sched.queue_len(adapter_id) == 0 and sched.inflight_count(adapter_id) == 0:
                sched.deactivate(adapter_id)
        for adapter_id in list(sched.active_bg):
            if sched.queue_len(adapter_id) == 0 and sched.inflight_count(adapter_id) == 0:
                sched.deactivate(adapter_id)
        for adapter_id in sched.inflight.keys():
            sched.activate(adapter_id)
        vip_backlog = {
            a
            for a in sched.adapters_with_backlog_or_inflight()
            if sched.adapter_class.get(a) == "VIP"
        }
        w_vip = len(vip_backlog)
        if w_vip < self.k:
            for adapter_id in vip_backlog:
                if len(sched.resident_set()) >= self.k:
                    break
                sched.activate(adapter_id)
            self._fill_active_bg(self.k - len(sched.active_vip))
        else:
            self.bg_paused = True
            for adapter_id in list(sched.active_bg):
                if sched.inflight_count(adapter_id) == 0:
                    sched.deactivate(adapter_id)
            self._fill_active_vip(self.k)

    def _fill_active_vip(self, target: int) -> None:
        sched = self.scheduler
        while len(sched.active_vip) < target and len(sched.resident_set()) < self.k:
            candidate = self._next_rr_candidate("VIP")
            if candidate is None:
                return
            sched.activate(candidate)

    def _fill_active_bg(self, target_bg: int) -> None:
        sched = self.scheduler
        while len(sched.active_bg) < target_bg and len(sched.resident_set()) < self.k:
            candidate = self._next_rr_candidate("BG")
            if candidate is None:
                return
            sched.activate(candidate)

    def _next_rr_candidate(self, cls: str) -> Optional[str]:
        sched = self.scheduler
        order = sched.adapter_order
        if not order:
            return None
        if cls == "VIP":
            idx = self.vip_rr_idx
        else:
            idx = self.bg_rr_idx
        n = len(order)
        for _ in range(n):
            adapter_id = order[idx % n]
            idx += 1
            if sched.adapter_class.get(adapter_id) != cls:
                continue
            if adapter_id in sched.resident_set():
                continue
            if sched.queue_len(adapter_id) == 0:
                continue
            if cls == "VIP":
                self.vip_rr_idx = idx % n
            else:
                self.bg_rr_idx = idx % n
            return adapter_id
        if cls == "VIP":
            self.vip_rr_idx = idx % n
        else:
            self.bg_rr_idx = idx % n
        return None

    def pick_next_adapter(self, now: float) -> Optional[str]:
        eligible: Set[str] = set(self.scheduler.active_vip)
        if not self.bg_paused:
            eligible = eligible.union(self.scheduler.active_bg)
        best_adapter = None
        best_ts = None
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
        return {"bg_paused": self.bg_paused}
