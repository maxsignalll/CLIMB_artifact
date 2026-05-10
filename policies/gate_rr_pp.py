import os
from typing import Optional

from .gate_rr import GateRRPolicy


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


class GateRRPPPolicy(GateRRPolicy):
    name = "gate_rr_pp"
    gate_enabled = True

    def __init__(self, k: int, bg_cap: int, cluster_q: int) -> None:
        super().__init__(k, bg_cap, cluster_q)
        self.mode = "SLACK"
        self.slack_counter = 0
        self.slack_confirm_ticks = _env_int("CLIMB_GATERRPP_SLACK_CONFIRM_TICKS", 2)
        self.d_vip = 0.0
        self.d_bg = 0.0
        self.q_vip = _env_float("CLIMB_GATERRPP_Q_VIP", 2.0)
        self.q_bg = _env_float("CLIMB_GATERRPP_Q_BG", 1.0)
        self.cost_per_req = _env_float("CLIMB_GATERRPP_COST", 1.0)
        self.rr_disp_vip = 0
        self.rr_disp_bg = 0
        self.enable_rescue = _env_bool("CLIMB_GATERRPP_ENABLE_RESCUE", False)
        self.rescue_boost = _env_float("CLIMB_GATERRPP_RESCUE_BOOST", 2.0)
        self.rescue_window_dispatches = _env_int(
            "CLIMB_GATERRPP_RESCUE_WINDOW", 8
        )
        self.rescue_remaining = 0
        self.rescue_on = False
        self.rescue_tau_ms = _env_float("CLIMB_GATERRPP_RESCUE_TAU_MS", 0.0)
        self.vip_hol_age_ms = 0.0
        self.swap_rate_1s: Optional[float] = None
        self._last_swap_ts: Optional[float] = None
        self._last_swap_count: Optional[int] = None
        self._last_w = 0
        self._last_w_vip = 0
        self._last_w_bg = 0
        self._last_backlog_vip = 0
        self._last_backlog_bg = 0
        self._last_q_vip_eff = self.q_vip

    def attach(self, scheduler) -> None:
        super().attach(scheduler)
        if self.rescue_tau_ms <= 0:
            self.rescue_tau_ms = 0.5 * self.scheduler.vip_slo_ms

    def _compute_w(self) -> tuple[int, int, int]:
        sched = self.scheduler
        adapters = sched.adapters_with_backlog_or_inflight()
        w_vip = 0
        w_bg = 0
        for adapter_id in adapters:
            if sched.adapter_class.get(adapter_id) == "VIP":
                w_vip += 1
            else:
                w_bg += 1
        return len(adapters), w_vip, w_bg

    def _update_swap_rate(self, now: float) -> None:
        if self._last_swap_ts is None:
            self._last_swap_ts = now
            self._last_swap_count = self.scheduler.swap_event_count
            return
        elapsed = max(1e-6, now - self._last_swap_ts)
        delta = self.scheduler.swap_event_count - (self._last_swap_count or 0)
        self.swap_rate_1s = delta / elapsed
        self._last_swap_ts = now
        self._last_swap_count = self.scheduler.swap_event_count

    def _update_mode(self, w_total: int) -> None:
        if w_total > self.k:
            self.slack_counter = 0
            self.mode = "BINDING"
        else:
            self.slack_counter += 1
            if self.slack_counter >= self.slack_confirm_ticks:
                self.mode = "SLACK"

    def _backlog_totals(self) -> tuple[int, int]:
        sched = self.scheduler
        vip_total = 0
        bg_total = 0
        for adapter_id, queue in sched.queues.items():
            if not queue:
                continue
            if sched.adapter_class.get(adapter_id) == "VIP":
                vip_total += len(queue)
            else:
                bg_total += len(queue)
        return vip_total, bg_total

    def _class_hol_age_ms(self, cls: str, now: float) -> float:
        sched = self.scheduler
        best = None
        for adapter_id, queue in sched.queues.items():
            if not queue:
                continue
            if sched.adapter_class.get(adapter_id) != cls:
                continue
            age = max(0.0, (now - queue[0].arrival_ts) * 1000.0)
            if best is None or age > best:
                best = age
        return best or 0.0

    def _vip_hol_age_ms(self, now: float) -> float:
        return self._class_hol_age_ms("VIP", now)

    def _active_has_backlog(self, cls: str) -> bool:
        sched = self.scheduler
        active = sched.active_vip if cls == "VIP" else sched.active_bg
        for adapter_id in active:
            if sched.queue_len(adapter_id) > 0:
                return True
        return False

    def _class_backlog_total(self, cls: str) -> int:
        if cls == "VIP":
            return self._last_backlog_vip
        return self._last_backlog_bg

    def _pick_active_rr(self, cls: str) -> Optional[str]:
        sched = self.scheduler
        order = sched.adapter_order
        if not order:
            return None
        if cls == "VIP":
            idx = self.rr_disp_vip
            active = sched.active_vip
        else:
            idx = self.rr_disp_bg
            active = sched.active_bg
        n = len(order)
        for _ in range(n):
            adapter_id = order[idx % n]
            idx += 1
            if sched.adapter_class.get(adapter_id) != cls:
                continue
            if adapter_id not in active:
                continue
            if sched.queue_len(adapter_id) == 0:
                continue
            if cls == "VIP":
                self.rr_disp_vip = idx % n
            else:
                self.rr_disp_bg = idx % n
            return adapter_id
        if cls == "VIP":
            self.rr_disp_vip = idx % n
        else:
            self.rr_disp_bg = idx % n
        return None

    def _pick_class(self, now: float) -> Optional[str]:
        vip_ready = self._class_backlog_total("VIP") > 0
        bg_ready = self._class_backlog_total("BG") > 0 and not self.bg_paused
        if not vip_ready and not bg_ready:
            return None

        q_vip_eff = self.q_vip * (self.rescue_boost if self.rescue_on else 1.0)
        self._last_q_vip_eff = q_vip_eff

        if vip_ready:
            self.d_vip += q_vip_eff
        if bg_ready:
            self.d_bg += self.q_bg

        if vip_ready and not bg_ready:
            chosen = "VIP"
        elif bg_ready and not vip_ready:
            chosen = "BG"
        else:
            if self.d_vip == self.d_bg:
                vip_age = self._class_hol_age_ms("VIP", now)
                bg_age = self._class_hol_age_ms("BG", now)
                chosen = "BG" if bg_age > vip_age else "VIP"
            else:
                chosen = "VIP" if self.d_vip > self.d_bg else "BG"

        if chosen == "VIP":
            self.d_vip = max(0.0, self.d_vip - self.cost_per_req)
        else:
            self.d_bg = max(0.0, self.d_bg - self.cost_per_req)
        return chosen

    def update_active_sets(self, now: float) -> None:
        w_total, w_vip, w_bg = self._compute_w()
        self._last_w = w_total
        self._last_w_vip = w_vip
        self._last_w_bg = w_bg
        self._last_backlog_vip, self._last_backlog_bg = self._backlog_totals()
        self._update_swap_rate(now)
        self._update_mode(w_total)

        if self.mode == "SLACK":
            self.d_vip = 0.0
            self.d_bg = 0.0
            self.rescue_on = False
            self.rescue_remaining = 0
            super().update_active_sets(now)
            return

        super().update_active_sets(now)

    def pick_next_adapter(self, now: float) -> Optional[str]:
        if self.mode == "SLACK":
            return super().pick_next_adapter(now)

        if self.enable_rescue:
            self.vip_hol_age_ms = self._vip_hol_age_ms(now)
            if self.rescue_on:
                if self.rescue_remaining <= 0:
                    self.rescue_on = False
            elif self.vip_hol_age_ms > self.rescue_tau_ms:
                self.rescue_on = True
                self.rescue_remaining = self.rescue_window_dispatches
        else:
            self.vip_hol_age_ms = 0.0
            self.rescue_on = False
            self.rescue_remaining = 0

        chosen_class = self._pick_class(now)
        if chosen_class is None:
            return None
        adapter_id = self._pick_active_rr(chosen_class)
        if adapter_id is None:
            if chosen_class == "VIP":
                self._fill_active_vip(len(self.scheduler.active_vip) + 1)
            elif not self.bg_paused:
                self._fill_active_bg(len(self.scheduler.active_bg) + 1)
            adapter_id = self._pick_active_rr(chosen_class)
        if adapter_id is None:
            return None

        if self.rescue_on:
            self.rescue_remaining -= 1
            if self.rescue_remaining <= 0:
                self.rescue_on = False
        return adapter_id

    def snapshot(self, now: float):
        return {
            "mode": self.mode,
            "slack_counter": self.slack_counter,
            "d_vip": self.d_vip,
            "d_bg": self.d_bg,
            "q_vip": self.q_vip,
            "q_bg": self.q_bg,
            "q_vip_effective": self._last_q_vip_eff,
            "bg_paused": self.bg_paused,
            "rescue_on": self.rescue_on,
            "vip_hol_age_ms": self.vip_hol_age_ms,
            "swap_rate_1s": self.swap_rate_1s,
            "backlog_vip_total": self._last_backlog_vip,
            "backlog_bg_total": self._last_backlog_bg,
        }
