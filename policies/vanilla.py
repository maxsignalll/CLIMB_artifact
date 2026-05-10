from typing import Optional

from ingress.policies.base import PolicyBase


class VanillaPolicy(PolicyBase):
    name = "vanilla"
    gate_enabled = False

    def pick_next_adapter(self, now: float) -> Optional[str]:
        best_adapter = None
        best_ts = None
        for adapter_id, queue in self.scheduler.queues.items():
            if not queue:
                continue
            ts = queue[0].arrival_ts
            if best_ts is None or ts < best_ts:
                best_ts = ts
                best_adapter = adapter_id
        return best_adapter
