import os
import time
from typing import Optional

from .gate_rr import GateRRPolicy


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


class GateUPolicy(GateRRPolicy):
    name = "gate_u"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.urgency_alpha = _env_float("CLIMB_GATEU_ALPHA", 1.0)

    def _fill_active_bg(self, target_bg: int) -> None:
        sched = self.scheduler
        while len(sched.active_bg) < target_bg and len(sched.resident_set()) < self.k:
            candidate = self._pick_urgent_bg()
            if candidate is None:
                return
            sched.activate(candidate)

    def _pick_urgent_bg(self) -> Optional[str]:
        sched = self.scheduler
        order = sched.adapter_order
        if not order:
            return None
        now = time.time()
        best_adapter = None
        best_urgency = None
        for adapter_id in order:
            if sched.adapter_class.get(adapter_id) != "BG":
                continue
            if adapter_id in sched.resident_set():
                continue
            queue = sched.queues.get(adapter_id)
            if not queue:
                continue
            hol_age_ms = max(0.0, (now - queue[0].arrival_ts) * 1000.0)
            urgency = max(hol_age_ms, self.urgency_alpha * len(queue))
            if best_urgency is None or urgency > best_urgency:
                best_urgency = urgency
                best_adapter = adapter_id
        return best_adapter

    def snapshot(self, now: float):
        data = super().snapshot(now)
        data["gate_u_alpha"] = self.urgency_alpha
        return data
