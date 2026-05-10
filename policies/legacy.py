import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

from .base import PolicyBase


@dataclass
class TickContext:
    now: float
    contention: bool
    did_prune_empty: bool = False


class LegacyPolicy(PolicyBase):
    """Legacy policy variants (not used by mainline CLIMB runs)."""

    name = "legacy"

    def __init__(
        self,
        k: int,
        bg_cap: int,
        cluster_q: int,
        *,
        quantum: float = 1.0,
        lease_q: int = 4,
        switch_budget: int = 4,
        cooldown_sec: float = 0.5,
        epoch_sec: float = 1.0,
        vip_boost: float = 1.0,
        disable_hard_gate: bool = False,
        disable_deficit: bool = False,
        disable_stability: bool = False,
    ) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.quantum = quantum
        self.lease_q = lease_q
        self.switch_budget = switch_budget
        self.cooldown_sec = cooldown_sec
        self.epoch_sec = epoch_sec
        self.vip_boost = vip_boost
        self.disable_hard_gate = disable_hard_gate
        self.disable_deficit = disable_deficit
        self.disable_stability = disable_stability
        self.gate_enabled = not disable_hard_gate
        self.epoch_start = time.time()
        self.epoch_idx = 0
        self.switch_budget_left = switch_budget
        self.cooldown_until = 0.0
        self.current_adapter: Optional[str] = None
        self.rr_ptr = 0
        self.non_preempt_violations = 0
        self.switch_blocked_budget = 0
        self.switch_blocked_cooldown = 0
        self.activate_attempted_total = 0
        self.activate_attempted_fill = 0
        self.activate_attempted_replace = 0
        self.activate_accepted_total = 0
        self.activate_blocked_budget = 0
        self.activate_blocked_cooldown = 0
        self.activate_blocked_gate = 0
        self.activate_bypassed_non_contention = 0
        self.activate_bypassed_liveness = 0
        self.deactivate_empty_prune_count = 0
        self.prune_then_fill_count = 0
        self.contention_ticks = 0
        self.total_ticks = 0
        self.idle_grace_ticks = 2
        self.idle_empty_ticks: Dict[str, int] = {}
        self.next_eligible_at: Dict[str, float] = {}

    def on_arrival(self, req) -> None:
        self._ensure_state(req.adapter_id)

    def on_dispatch(self, req) -> None:
        sched = self.scheduler
        if not self.disable_deficit:
            sched.deficit[req.adapter_id] = max(
                0.0, sched.deficit.get(req.adapter_id, 0.0) - 1.0
            )
        lease_left = sched.lease_left.get(req.adapter_id, 0)
        if lease_left > 0:
            sched.lease_left[req.adapter_id] = lease_left - 1
        if sched.lease_left.get(req.adapter_id, 0) <= 0 and self.current_adapter == req.adapter_id:
            self.current_adapter = None
        sched.current_adapter = self.current_adapter
        sched.cluster_left = sched.lease_left.get(self.current_adapter) if self.current_adapter else None

    def update_active_sets(self, now: float) -> None:
        sched = self.scheduler
        self._reset_epoch(now)
        self._refresh_cooldown(now)
        self._ensure_backlog_state()
        self._add_deficit()
        ctx = self._make_tick_context(now)

        for adapter_id in list(sched.inflight.keys()):
            self._activate(adapter_id, now, force=True)

        vip_backlog, bg_backlog = self._backlog_by_class()
        w_vip = len(vip_backlog)

        if self.disable_hard_gate:
            self._fill_active_set(vip_backlog, bg_backlog, now, ctx, enforce_gate=False)
        else:
            if w_vip <= self.k:
                self._fill_active_set(vip_backlog, bg_backlog, now, ctx, enforce_gate=True)
            else:
                self._fill_active_vip_only(vip_backlog, now, ctx)

        sched.current_adapter = self.current_adapter
        sched.cluster_left = (
            sched.lease_left.get(self.current_adapter) if self.current_adapter else None
        )
        sched.cooldown = self._cooldown_left(now)

    def pick_next_adapter(self, now: float) -> Optional[str]:
        sched = self.scheduler
        eligible = self._eligible_adapters()
        vip_candidates = [
            a
            for a in eligible
            if sched.adapter_class.get(a) == "VIP" and sched.queue_len(a) > 0
        ]
        bg_candidates = [
            a
            for a in eligible
            if sched.adapter_class.get(a) == "BG" and sched.queue_len(a) > 0
        ]

        if self.current_adapter in eligible and sched.queue_len(self.current_adapter) > 0:
            lease_left = sched.lease_left.get(self.current_adapter, 0)
            if lease_left > 0:
                if sched.adapter_class.get(self.current_adapter) == "VIP" or not vip_candidates:
                    return self.current_adapter

        if not vip_candidates and not bg_candidates:
            return None

        if self.disable_deficit:
            if vip_candidates:
                adapter_id = self._rr_pick(vip_candidates)
            else:
                adapter_id = self._rr_pick(bg_candidates)
        else:
            adapter_id = self._pick_by_deficit(vip_candidates + bg_candidates)

        if adapter_id is None:
            return None

        if self.current_adapter != adapter_id:
            if not self.disable_stability and self.lease_q > 0:
                sched.lease_left[adapter_id] = self.lease_q
            self.current_adapter = adapter_id
        return adapter_id

    def snapshot(self, now: float) -> Dict[str, Optional[float]]:
        contention_ratio = (
            self.contention_ticks / self.total_ticks if self.total_ticks > 0 else None
        )
        return {
            "epoch_id": self.epoch_idx,
            "switch_budget_left": self.switch_budget_left,
            "switch_budget": self.switch_budget,
            "switch_blocked_budget": self.switch_blocked_budget,
            "switch_blocked_cooldown": self.switch_blocked_cooldown,
            "activate_attempted_total": self.activate_attempted_total,
            "activate_attempted_fill": self.activate_attempted_fill,
            "activate_attempted_replace": self.activate_attempted_replace,
            "activate_accepted_total": self.activate_accepted_total,
            "activate_blocked_budget": self.activate_blocked_budget,
            "activate_blocked_cooldown": self.activate_blocked_cooldown,
            "activate_blocked_gate": self.activate_blocked_gate,
            "activate_bypassed_non_contention": self.activate_bypassed_non_contention,
            "activate_bypassed_liveness": self.activate_bypassed_liveness,
            "deactivate_empty_prune_count": self.deactivate_empty_prune_count,
            "prune_then_fill_count": self.prune_then_fill_count,
            "contention_ticks": self.contention_ticks,
            "total_ticks": self.total_ticks,
            "contention_time_ratio": contention_ratio,
            "cooldown_sec": self.cooldown_sec,
            "epoch_sec": self.epoch_sec,
            "quantum": self.quantum,
            "lease_q": 0 if self.disable_stability else self.lease_q,
            "vip_boost": self.vip_boost,
            "disable_hard_gate": self.disable_hard_gate,
            "disable_deficit": self.disable_deficit,
            "disable_stability": self.disable_stability,
            "non_preempt_violations": self.non_preempt_violations,
        }

    def _ensure_state(self, adapter_id: str) -> None:
        sched = self.scheduler
        sched.deficit.setdefault(adapter_id, 0.0)
        sched.lease_left.setdefault(adapter_id, 0)

    def _ensure_backlog_state(self) -> None:
        sched = self.scheduler
        for adapter_id in sched.adapters_with_backlog_or_inflight():
            self._ensure_state(adapter_id)

    def _make_tick_context(self, now: float) -> TickContext:
        sched = self.scheduler
        total = len(sched.adapters_with_backlog_or_inflight())
        contention = total > self.k
        self.total_ticks += 1
        if contention:
            self.contention_ticks += 1
        return TickContext(now=now, contention=contention)

    def _idle_grace_passed(self, adapter_id: str, *, has_work: bool) -> bool:
        if has_work:
            if adapter_id in self.idle_empty_ticks:
                self.idle_empty_ticks[adapter_id] = 0
            return False
        ticks = self.idle_empty_ticks.get(adapter_id, 0) + 1
        self.idle_empty_ticks[adapter_id] = ticks
        return ticks >= self.idle_grace_ticks

    def _add_deficit(self) -> None:
        if self.disable_deficit:
            return
        sched = self.scheduler
        for adapter_id in sched.adapters_with_backlog():
            sched.deficit[adapter_id] = sched.deficit.get(adapter_id, 0.0) + self.quantum

    def _eligible_adapters(self) -> List[str]:
        sched = self.scheduler
        if self.disable_hard_gate:
            return list(sched.adapters_with_backlog())
        return list(sched.active_vip.union(sched.active_bg))

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

    def _fill_active_set(
        self,
        vip_backlog: Set[str],
        bg_backlog: Set[str],
        now: float,
        ctx: TickContext,
        *,
        enforce_gate: bool,
    ) -> None:
        sched = self.scheduler
        self._trim_active_set(sched.active_vip, vip_backlog, now, ctx)
        self._trim_active_set(sched.active_bg, bg_backlog, now, ctx)
        for adapter_id in self._rank_activation_candidates(
            vip_backlog, class_filter="VIP", now=now
        ):
            self._maybe_activate(
                adapter_id,
                now,
                ctx,
                enforce_gate=enforce_gate,
                reason="FILL",
            )
        self._shrink_active_set(
            sched.active_vip,
            len(vip_backlog),
            now,
            ctx,
            allow_backlog=False,
        )
        target_bg = (
            max(0, self.k - min(len(vip_backlog), self.k))
            if enforce_gate
            else len(bg_backlog)
        )
        self._shrink_active_set(
            sched.active_bg,
            target_bg,
            now,
            ctx,
            allow_backlog=True,
        )
        self._fill_active_bg(bg_backlog, target_bg, now, ctx, enforce_gate=enforce_gate)

    def _fill_active_vip_only(self, vip_backlog: Set[str], now: float, ctx: TickContext) -> None:
        sched = self.scheduler
        self._trim_active_set(sched.active_bg, set(), now, ctx, force_remove=True)
        self._trim_active_set(sched.active_vip, vip_backlog, now, ctx)
        self._shrink_active_set(
            sched.active_bg,
            0,
            now,
            ctx,
            allow_backlog=True,
        )
        self._shrink_active_set(
            sched.active_vip,
            self.k,
            now,
            ctx,
            allow_backlog=True,
        )
        target = self.k
        while len(sched.active_vip) < target:
            candidate = self._pick_activation_candidate(
                vip_backlog, class_filter="VIP", now=now
            )
            if candidate is None:
                return
            if not self._maybe_activate(
                candidate,
                now,
                ctx,
                enforce_gate=True,
                reason="FILL",
            ):
                return

    def _fill_active_bg(
        self,
        bg_backlog: Set[str],
        target: int,
        now: float,
        ctx: TickContext,
        *,
        enforce_gate: bool,
    ) -> None:
        sched = self.scheduler
        while len(sched.active_bg) < target:
            candidate = self._pick_activation_candidate(
                bg_backlog, class_filter="BG", now=now
            )
            if candidate is None:
                return
            if not self._maybe_activate(
                candidate,
                now,
                ctx,
                enforce_gate=enforce_gate,
                reason="FILL",
            ):
                return

    def _trim_active_set(
        self,
        active_set: Set[str],
        backlog: Set[str],
        now: float,
        ctx: TickContext,
        *,
        force_remove: bool = False,
    ) -> None:
        sched = self.scheduler
        resident_full = len(sched.resident_set()) >= self.k
        backlog_outside = any(
            adapter_id not in sched.resident_set() and sched.queue_len(adapter_id) > 0
            for adapter_id in backlog
        )
        pressure = resident_full and backlog_outside
        for adapter_id in list(active_set):
            if adapter_id in backlog:
                self._idle_grace_passed(
                    adapter_id,
                    has_work=True,
                )
                continue
            if sched.inflight_count(adapter_id) > 0:
                self._idle_grace_passed(
                    adapter_id,
                    has_work=True,
                )
                continue
            if sched.queue_len(adapter_id) > 0 and sched.lease_left.get(adapter_id, 0) > 0:
                self._idle_grace_passed(
                    adapter_id,
                    has_work=True,
                )
                continue
            if not force_remove and sched.queue_len(adapter_id) > 0:
                self._idle_grace_passed(
                    adapter_id,
                    has_work=True,
                )
                continue
            if force_remove or pressure or self._idle_grace_passed(adapter_id, has_work=False):
                if self._deactivate(adapter_id, now, ignore_stability=True):
                    ctx.did_prune_empty = True
                    self.deactivate_empty_prune_count += 1

    def _shrink_active_set(
        self,
        active_set: Set[str],
        target: int,
        now: float,
        ctx: TickContext,
        *,
        allow_backlog: bool,
    ) -> None:
        while len(active_set) > target:
            candidate = self._pick_eviction_candidate(active_set, allow_backlog=allow_backlog)
            if candidate is None:
                return
            if not self._deactivate(candidate, now, ignore_stability=True):
                return

    def _pick_eviction_candidate(
        self, active_set: Set[str], *, allow_backlog: bool
    ) -> Optional[str]:
        sched = self.scheduler
        candidates = []
        for adapter_id in active_set:
            if sched.inflight_count(adapter_id) > 0:
                continue
            if sched.lease_left.get(adapter_id, 0) > 0:
                continue
            if not allow_backlog and sched.queue_len(adapter_id) > 0:
                continue
            candidates.append(adapter_id)
        if not candidates:
            return None
        if self.disable_deficit:
            return self._rr_pick(candidates)
        return self._pick_lowest_deficit(candidates)

    def _pick_lowest_deficit(self, candidates: List[str]) -> Optional[str]:
        sched = self.scheduler
        min_score = None
        pool = []
        for adapter_id in candidates:
            score = sched.deficit.get(adapter_id, 0.0)
            if min_score is None or score < min_score:
                min_score = score
                pool = [adapter_id]
            elif score == min_score:
                pool.append(adapter_id)
        return self._rr_pick(pool)

    def _pick_activation_candidate(
        self, backlog: Set[str], class_filter: Optional[str], now: float
    ) -> Optional[str]:
        ranked = self._rank_activation_candidates(backlog, class_filter=class_filter, now=now)
        return ranked[0] if ranked else None

    def _rank_activation_candidates(
        self, backlog: Set[str], class_filter: Optional[str], now: float
    ) -> List[str]:
        sched = self.scheduler
        candidates = [
            a
            for a in backlog
            if sched.queue_len(a) > 0 and a not in sched.resident_set()
        ]
        if class_filter:
            candidates = [
                a for a in candidates if sched.adapter_class.get(a) == class_filter
            ]
        if not candidates:
            return []
        def score(adapter_id: str) -> tuple:
            queue = sched.queues.get(adapter_id)
            if queue:
                head_ts = queue[0].arrival_ts
            else:
                head_ts = sched.last_arrival_ts.get(adapter_id, now)
            age = max(0.0, now - head_ts)
            qlen = sched.queue_len(adapter_id)
            return (age, qlen, adapter_id)
        ranked = sorted(candidates, key=score, reverse=True)
        return ranked

    def _pick_by_deficit(self, candidates: List[str]) -> Optional[str]:
        if not candidates:
            return None
        sched = self.scheduler
        scores = {}
        max_score = None
        for adapter_id in candidates:
            deficit = sched.deficit.get(adapter_id, 0.0)
            if sched.adapter_class.get(adapter_id) == "VIP":
                deficit += self.vip_boost
            scores[adapter_id] = deficit
            if max_score is None or deficit > max_score:
                max_score = deficit
        top = [a for a, score in scores.items() if score == max_score]
        return self._rr_pick(top)

    def _rr_pick(self, candidates: Iterable[str]) -> Optional[str]:
        sched = self.scheduler
        order = sched.adapter_order
        if not order:
            return None
        candidate_set = set(candidates)
        if not candidate_set:
            return None
        n = len(order)
        for _ in range(n):
            adapter_id = order[self.rr_ptr % n]
            self.rr_ptr = (self.rr_ptr + 1) % n
            if adapter_id in candidate_set:
                return adapter_id
        return None

    def _activate(
        self,
        adapter_id: str,
        now: float,
        *,
        enforce_gate: bool = True,
        force: bool = False,
    ) -> bool:
        sched = self.scheduler
        if adapter_id in sched.resident_set():
            sched.activate(adapter_id)
            return True
        if enforce_gate and len(sched.resident_set()) >= self.k and not force:
            return False
        sched.activate(adapter_id)
        if not self.disable_stability and self.lease_q > 0:
            sched.lease_left[adapter_id] = self.lease_q
        return True

    def _deactivate(self, adapter_id: str, now: float, *, ignore_stability: bool = True) -> bool:
        sched = self.scheduler
        if sched.inflight_count(adapter_id) > 0:
            self.non_preempt_violations += 1
            return False
        if not ignore_stability:
            reason = self._switch_block_reason(now)
            if reason:
                self._record_switch_block(reason)
                return False
        sched.deactivate(adapter_id)
        sched.lease_left[adapter_id] = 0
        if self.current_adapter == adapter_id:
            self.current_adapter = None
        return True

    def _maybe_activate(
        self,
        adapter_id: str,
        now: float,
        ctx: TickContext,
        *,
        enforce_gate: bool,
        reason: str,
    ) -> bool:
        sched = self.scheduler
        if adapter_id in sched.resident_set():
            sched.activate(adapter_id)
            return True
        if enforce_gate and len(sched.resident_set()) >= self.k:
            self.activate_blocked_gate += 1
            return False
        next_ts = self.next_eligible_at.get(adapter_id, 0.0)
        if next_ts > now and self._has_resident_work():
            return False
        replace = adapter_id not in sched.resident_set() and len(sched.resident_set()) >= self.k
        self.activate_attempted_total += 1
        if replace or reason == "REPLACE":
            self.activate_attempted_replace += 1
        else:
            self.activate_attempted_fill += 1
        must_guard = ctx.contention or ctx.did_prune_empty or replace
        if must_guard:
            block_reason = self._switch_block_reason(now)
            if block_reason:
                if not self._has_resident_work():
                    self.activate_bypassed_liveness += 1
                else:
                    if block_reason == "budget":
                        self.activate_blocked_budget += 1
                        self.next_eligible_at[adapter_id] = max(
                            self.next_eligible_at.get(adapter_id, 0.0),
                            self.epoch_start + self.epoch_sec,
                        )
                    elif block_reason == "cooldown":
                        self.activate_blocked_cooldown += 1
                        self.next_eligible_at[adapter_id] = max(
                            self.next_eligible_at.get(adapter_id, 0.0),
                            self.cooldown_until,
                        )
                    self._record_switch_block(block_reason)
                    return False
        else:
            self.activate_bypassed_non_contention += 1
        if not self._activate(adapter_id, now, enforce_gate=enforce_gate, force=False):
            return False
        self.next_eligible_at.pop(adapter_id, None)
        self.activate_accepted_total += 1
        if ctx.did_prune_empty:
            self.prune_then_fill_count += 1
        if must_guard:
            self._consume_switch(now)
        return True

    def _reset_epoch(self, now: float) -> None:
        if self.disable_stability:
            self.switch_budget_left = None
            return
        if now - self.epoch_start >= self.epoch_sec:
            self.epoch_start = now
            self.epoch_idx += 1
            self.switch_budget_left = self.switch_budget

    def _refresh_cooldown(self, now: float) -> None:
        if self.disable_stability:
            self.cooldown_until = 0.0

    def _cooldown_left(self, now: float) -> float:
        if self.disable_stability:
            return 0.0
        return max(0.0, self.cooldown_until - now)

    def _can_switch(self, now: float) -> bool:
        return self._switch_block_reason(now) is None

    def _switch_block_reason(self, now: float) -> Optional[str]:
        if self.disable_stability:
            return None
        if self.switch_budget_left is not None and self.switch_budget_left <= 0:
            return "budget"
        if self._cooldown_left(now) > 0:
            return "cooldown"
        return None

    def _record_switch_block(self, reason: str) -> None:
        if reason == "budget":
            self.switch_blocked_budget += 1
        elif reason == "cooldown":
            self.switch_blocked_cooldown += 1

    def _consume_switch(self, now: float) -> None:
        if self.disable_stability:
            return
        if self.switch_budget_left is not None:
            self.switch_budget_left = max(0, self.switch_budget_left - 1)
        self.cooldown_until = now + self.cooldown_sec

    def _has_resident_work(self) -> bool:
        sched = self.scheduler
        for adapter_id in sched.resident_set():
            if sched.queue_len(adapter_id) > 0 or sched.inflight_count(adapter_id) > 0:
                return True
        return False
