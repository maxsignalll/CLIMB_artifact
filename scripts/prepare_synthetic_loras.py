#!/usr/bin/env python3
"""Prepare synthetic LoRA adapters for the optional GPU rerun.

The generated adapters are not trained task adapters. They are intended to
recreate adapter residency pressure for the public rerun path while keeping
model weights and large adapter directories out of the artifact repository.
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import torch
from peft import LoraConfig, get_peft_model
from transformers import AutoModelForCausalLM


def parse_csv(value: str) -> list[str]:
    items = [item.strip() for item in value.split(",")]
    return [item for item in items if item]


def adapter_ready(path: Path) -> bool:
    return (path / "adapter_config.json").exists() and (
        path / "adapter_model.safetensors"
    ).exists()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate synthetic LoRA adapters for CLIMB GPU reruns."
    )
    parser.add_argument("--model", required=True, help="Base HuggingFace model path.")
    parser.add_argument("--out", required=True, help="Output adapter directory.")
    parser.add_argument(
        "--names",
        default="vip,bg01,bg02,bg03,bg04,bg05,bg06,bg07",
        help="Comma-separated adapter names to create.",
    )
    parser.add_argument("--rank", type=int, default=128, help="LoRA rank.")
    parser.add_argument(
        "--alpha",
        type=int,
        default=None,
        help="LoRA alpha. Defaults to 2 * rank.",
    )
    parser.add_argument(
        "--target-modules",
        default="q_proj,k_proj,v_proj,o_proj",
        help="Comma-separated target module names.",
    )
    args = parser.parse_args()

    names = parse_csv(args.names)
    targets = parse_csv(args.target_modules)
    if not names:
        raise SystemExit("--names must contain at least one adapter name")
    if not targets:
        raise SystemExit("--target-modules must contain at least one module")

    out_dir = Path(args.out).expanduser().resolve()
    first_dir = out_dir / names[0]
    if adapter_ready(first_dir) and all(adapter_ready(out_dir / name) for name in names):
        print(f"SYNTHETIC_LORA_OK existing={out_dir}")
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"Loading base model from {args.model}")
    model = AutoModelForCausalLM.from_pretrained(
        args.model,
        torch_dtype=torch.float16,
        low_cpu_mem_usage=True,
        device_map={"": "cpu"},
    )
    config = LoraConfig(
        r=args.rank,
        lora_alpha=args.alpha if args.alpha is not None else args.rank * 2,
        target_modules=targets,
        lora_dropout=0.0,
        bias="none",
        task_type="CAUSAL_LM",
    )
    peft_model = get_peft_model(model, config)
    peft_model.save_pretrained(first_dir, safe_serialization=True)

    for name in names[1:]:
        dst = out_dir / name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(first_dir, dst)

    missing = [str(out_dir / name) for name in names if not adapter_ready(out_dir / name)]
    if missing:
        raise SystemExit("Missing generated adapter files under: " + ", ".join(missing))

    print(f"SYNTHETIC_LORA_OK out={out_dir} count={len(names)} rank={args.rank}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
