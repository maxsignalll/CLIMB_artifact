from typing import List, Optional, Set

from .base import PolicyBase


class CapOnlyPolicy(PolicyBase):
    name = "cap_only"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.bg_order: List[str] = []

    def on_arrival(self, req) -> None:
        if req.cls == "BG" and req.adapter_id not in self.bg_order:
            self.bg_order.append(req.adapter_id)

    def update_active_sets(self, now: float) -> None:
        sched = self.scheduler
        for adapter_id in list(sched.active_vip):
            if sched.queue_len(adapter_id) == 0 and sched.inflight_count(adapter_id) == 0:
                sched.deactivate(adapter_id)
        for adapter_id in list(sched.active_bg):
            if sched.queue_len(adapter_id) == 0 and sched.inflight_count(adapter_id) == 0:
                sched.deactivate(adapter_id)
        for adapter_id in sched.adapters_with_backlog_or_inflight():
            if sched.adapter_class.get(adapter_id) == "VIP":
                sched.activate(adapter_id)
        for adapter_id in sched.inflight.keys():
            if sched.adapter_class.get(adapter_id) == "BG":
                sched.activate(adapter_id)
        self._fill_active_bg()

    def _fill_active_bg(self) -> None:
        sched = self.scheduler
        if self.bg_cap <= 0:
            return
        while len(sched.active_bg) < self.bg_cap:
            candidate = self._next_bg_candidate()
            if candidate is None:
                return
            sched.activate(candidate)

    def _next_bg_candidate(self) -> Optional[str]:
        sched = self.scheduler
        for adapter_id in self.bg_order:
            if adapter_id in sched.active_bg:
                continue
            if sched.queue_len(adapter_id) > 0:
                return adapter_id
        return None

    def pick_next_adapter(self, now: float) -> Optional[str]:
        eligible: Set[str] = set(self.scheduler.active_vip).union(self.scheduler.active_bg)
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
