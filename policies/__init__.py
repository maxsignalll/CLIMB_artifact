from .base import PolicyBase
from .vanilla import VanillaPolicy
from .cap_only import CapOnlyPolicy
from .gate_rr import GateRRPolicy
from .gate_rr_pp import GateRRPPPolicy
from .gate_u import GateUPolicy
from .gate_mix import GateMixPolicy
from .cache_aware import CacheAwarePolicy
from .legacy import LegacyPolicy
from .no_switch import NoSwitchPolicy

POLICY_REGISTRY = {
    "vanilla": VanillaPolicy,
    "cap_only": CapOnlyPolicy,
    "gate_rr": GateRRPolicy,
    "gate_rr_pp": GateRRPPPolicy,
    "gate_u": GateUPolicy,
    "gate_mix": GateMixPolicy,
    "cache_aware": CacheAwarePolicy,
    "legacy": LegacyPolicy,
    "no_switch": NoSwitchPolicy,
}

__all__ = [
    "PolicyBase",
    "VanillaPolicy",
    "CapOnlyPolicy",
    "GateRRPolicy",
    "GateRRPPPolicy",
    "GateUPolicy",
    "GateMixPolicy",
    "CacheAwarePolicy",
    "LegacyPolicy",
    "NoSwitchPolicy",
    "POLICY_REGISTRY",
]
