import os
import time
from typing import Dict, List, Optional

from .gate_rr import GateRRPolicy


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


class GateMixPolicy(GateRRPolicy):
    name = "gate_mix"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.urgency_alpha = _env_float("CLIMB_GATEMIX_ALPHA", 1.0)
        self.last_lambda: float = 0.0
        self.last_p_max: float = 0.0
        self.last_bg_backlog: int = 0

    def _fill_active_bg(self, target_bg: int) -> None:
        sched = self.scheduler
        while len(sched.active_bg) < target_bg and len(sched.resident_set()) < self.k:
            candidate = self._pick_mixed_bg()
            if candidate is None:
                return
            sched.activate(candidate)

    def _pick_mixed_bg(self) -> Optional[str]:
        sched = self.scheduler
        order = sched.adapter_order
        if not order:
            return None

        candidates: List[str] = []
        for adapter_id in order:
            if sched.adapter_class.get(adapter_id) != "BG":
                continue
            if adapter_id in sched.resident_set():
                continue
            if sched.queue_len(adapter_id) == 0:
                continue
            candidates.append(adapter_id)

        if not candidates:
            return None

        lam, p_max, total_bg = self._compute_bg_skew()
        rr_scores = self._rr_scores(candidates)
        urg_scores = self._urgency_scores(candidates)

        best_adapter = None
        best_score = None
        for adapter_id in candidates:
            rr_score = rr_scores.get(adapter_id, 0.0)
            urg_score = urg_scores.get(adapter_id, 0.0)
            score = (1.0 - lam) * rr_score + lam * urg_score
            if best_score is None or score > best_score:
                best_score = score
                best_adapter = adapter_id

        if best_adapter is None:
            return None

        self._advance_rr(best_adapter, order)
        self.last_lambda = lam
        self.last_p_max = p_max
        self.last_bg_backlog = total_bg
        return best_adapter

    def _compute_bg_skew(self) -> tuple[float, float, int]:
        sched = self.scheduler
        bg_adapters = [a for a in sched.adapter_order if sched.adapter_class.get(a) == "BG"]
        n = len(bg_adapters)
        if n <= 1:
            return 0.0, 0.0, 0
        total = 0
        max_backlog = 0
        for adapter_id in bg_adapters:
            backlog = sched.queue_len(adapter_id)
            total += backlog
            if backlog > max_backlog:
                max_backlog = backlog
        if total <= 0:
            return 0.0, 0.0, 0
        p_max = max_backlog / float(total)
        denom = 1.0 - 1.0 / n
        if denom <= 0:
            lam = 0.0
        else:
            lam = (p_max - 1.0 / n) / denom
        lam = max(0.0, min(1.0, lam))
        return lam, p_max, total

    def _rr_scores(self, candidates: List[str]) -> Dict[str, float]:
        sched = self.scheduler
        order = sched.adapter_order
        n = len(candidates)
        if n <= 0:
            return {}
        if n == 1:
            return {candidates[0]: 1.0}
        scores: Dict[str, float] = {}
        idx = self.bg_rr_idx % len(order) if order else 0
        seen = 0
        for _ in range(len(order)):
            adapter_id = order[idx % len(order)]
            idx += 1
            if adapter_id not in candidates:
                continue
            score = 1.0 - (seen / float(n - 1))
            scores[adapter_id] = score
            seen += 1
            if seen >= n:
                break
        return scores

    def _urgency_scores(self, candidates: List[str]) -> Dict[str, float]:
        sched = self.scheduler
        n = len(candidates)
        if n <= 0:
            return {}
        if n == 1:
            return {candidates[0]: 1.0}
        now = time.time()
        urgencies: Dict[str, float] = {}
        order_index = {a: i for i, a in enumerate(sched.adapter_order)}
        for adapter_id in candidates:
            queue = sched.queues.get(adapter_id)
            if not queue:
                urg = 0.0
            else:
                hol_age_ms = max(0.0, (now - queue[0].arrival_ts) * 1000.0)
                urg = max(hol_age_ms, self.urgency_alpha * len(queue))
            urgencies[adapter_id] = urg
        sorted_adapters = sorted(
            candidates,
            key=lambda a: (-urgencies.get(a, 0.0), order_index.get(a, 0)),
        )
        scores: Dict[str, float] = {}
        for rank, adapter_id in enumerate(sorted_adapters):
            score = 1.0 - (rank / float(n - 1))
            scores[adapter_id] = score
        return scores

    def _advance_rr(self, adapter_id: str, order: List[str]) -> None:
        try:
            idx = order.index(adapter_id)
        except ValueError:
            return
        self.bg_rr_idx = (idx + 1) % len(order)

    def snapshot(self, now: float):
        data = super().snapshot(now)
        data["gate_mix_lambda"] = self.last_lambda
        data["gate_mix_p_max"] = self.last_p_max
        data["gate_mix_bg_backlog"] = self.last_bg_backlog
        data["gate_mix_alpha"] = self.urgency_alpha
        return data
