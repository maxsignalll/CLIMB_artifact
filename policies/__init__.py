from ingress.policies.base import PolicyBase
from ingress.policies.vanilla import VanillaPolicy
from ingress.policies.cap_only import CapOnlyPolicy
from ingress.policies.gate_rr import GateRRPolicy
from ingress.policies.gate_rr_pp import GateRRPPPolicy
from ingress.policies.gate_u import GateUPolicy
from ingress.policies.gate_mix import GateMixPolicy
from ingress.policies.cache_aware import CacheAwarePolicy
from ingress.policies.legacy import LegacyPolicy
from ingress.policies.no_switch import NoSwitchPolicy

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
