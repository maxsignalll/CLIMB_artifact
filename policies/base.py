from typing import Dict, Optional


class PolicyBase:
    name = "base"
    gate_enabled = False

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        self.k = k
        self.bg_cap = bg_cap
        self.cluster_q = cluster_q
        self.scheduler = None

    def attach(self, scheduler) -> None:
        self.scheduler = scheduler

    def on_arrival(self, req) -> None:
        return None

    def on_finish(self, req) -> None:
        return None

    def on_dispatch(self, req) -> None:
        return None

    def update_active_sets(self, now: float) -> None:
        return None

    def pick_next_adapter(self, now: float) -> Optional[str]:
        return None

    def snapshot(self, now: float) -> Dict[str, Optional[float]]:
        return {}

    def lru_ranks(self) -> Dict[str, int]:
        return {}
