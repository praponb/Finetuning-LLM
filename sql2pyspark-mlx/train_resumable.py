"""Run `mlx_lm.lora -c lora_config.yaml` and resume automatically if Metal aborts the run.

On a base M2 the macOS GPU watchdog can kill a long training step ("Impacting Interactivity" /
kIOGPUCommandBufferCallbackErrorImpactingInteractivity), which mlx_lm cannot recover from. This wrapper
- runs with smaller Metal command buffers (MLX_MAX_OPS_PER_BUFFER / MLX_MAX_MB_PER_BUFFER), and
- after a crash, restarts from the newest checkpoint (`adapters/NNNNNNN_adapters.safetensors`) for the
  remaining iterations, continuing the cosine learning-rate schedule from where it stopped
  (the AdamW moments start fresh, which costs little this late in a LoRA run).

Iterations are micro-batches (with grad_accumulation_steps > 1 the optimizer updates every N of them);
the learning-rate schedule counts optimizer updates.

Usage (from sql2pyspark-mlx/):  python train_resumable.py [--config lora_config.yaml] [--max-restarts 6]
                                [--launcher script.py]   (a script with mlx_lm.lora's arguments, e.g. qwen35_pilot/lora_q35.py)
                                [--segment-val-batches N]  (smaller validation in resumed segments only)
"""
from __future__ import annotations

import argparse
import math
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent


def lr_at(update: int, sched: dict) -> float:
    """Learning rate after `update` optimizer updates for mlx_lm's warmup + cosine_decay schedule."""
    warmup = sched.get("warmup", 0)
    init = sched.get("warmup_init", 0.0)
    peak, decay_steps, end = sched["arguments"]
    if update < warmup:
        return init + (peak - init) * update / warmup
    t = min(update - warmup, decay_steps)
    return end + (peak - end) * 0.5 * (1 + math.cos(math.pi * t / decay_steps))


def newest_checkpoint(adapter_dir: Path):
    best = None
    for f in adapter_dir.glob("*_adapters.safetensors"):
        m = re.match(r"(\d+)_adapters\.safetensors", f.name)
        if m and (best is None or int(m.group(1)) > best[0]):
            best = (int(m.group(1)), f)
    return best


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="lora_config.yaml")
    ap.add_argument("--max-restarts", type=int, default=6)
    ap.add_argument("--launcher", default=None, help="python script to run instead of mlx_lm.lora (same arguments)")
    ap.add_argument("--segment-val-batches", type=int, default=None,
                    help="val_batches for resumed segments only (mlx-lm validates at a segment's iteration 1, so a "
                         "full validation after every restart is wasted; q9c lost ~1.4 h to 6 of them). Default: unchanged")
    args = ap.parse_args()

    os.chdir(HERE)
    cfg = yaml.safe_load(Path(args.config).read_text())
    total = cfg["iters"]
    accum = cfg.get("grad_accumulation_steps", 1)
    adapter_dir = Path(cfg["adapter_path"])
    env = dict(os.environ, MLX_MAX_OPS_PER_BUFFER=os.environ.get("MLX_MAX_OPS_PER_BUFFER", "2"),
               MLX_MAX_MB_PER_BUFFER=os.environ.get("MLX_MAX_MB_PER_BUFFER", "40"))
    lora = [sys.executable, args.launcher] if args.launcher else [str(Path(sys.executable).with_name("mlx_lm.lora"))]

    done, segment, run_cfg = 0, 1, Path(args.config)
    for attempt in range(args.max_restarts + 1):
        print(f"[train_resumable] segment {segment}: iterations {done + 1}..{total} ({run_cfg})", flush=True)
        rc = subprocess.run([*lora, "-c", str(run_cfg)], env=env).returncode
        seg_dir = Path(yaml.safe_load(run_cfg.read_text())["adapter_path"])
        if rc == 0:
            if seg_dir != adapter_dir:       # final weights of a resumed segment -> the normal location
                shutil.copy(seg_dir / "adapters.safetensors", adapter_dir / "adapters.safetensors")
            print(f"[train_resumable] finished all {total} iterations", flush=True)
            return
        ck = newest_checkpoint(seg_dir)
        if ck is None and done == 0:
            print("[train_resumable] crashed before the first checkpoint; restarting from scratch", flush=True)
        elif ck is not None:
            done += ck[0]
            # keep the checkpoint under its global iteration number (segment 1 already writes it there)
            dst = adapter_dir / f"{done:07d}_adapters.safetensors"
            if ck[1].resolve() != dst.resolve():
                shutil.copy(ck[1], dst)
        print(f"[train_resumable] exit code {rc}; {done}/{total} iterations saved", flush=True)
        remaining = total - done
        if remaining <= 0:
            return
        segment += 1
        seg_dir = HERE / f"{adapter_dir.name}_seg{segment}"
        seg_dir.mkdir(exist_ok=True)
        c = dict(cfg)
        c["iters"] = remaining
        c["adapter_path"] = str(seg_dir)
        # new batch order per segment: mlx-lm shuffles with np.random.seed(seed), so the same seed made every resumed
        # segment re-train the start of the same order (found 2026-11-15 on the Qwen3.5-35B run)
        c["seed"] = int(cfg.get("seed", 0)) + 1000 * segment
        if args.segment_val_batches:
            c["val_batches"] = args.segment_val_batches
        if done:
            c["resume_adapter_file"] = str(adapter_dir / f"{done:07d}_adapters.safetensors")
            sched = cfg["lr_schedule"]
            u, updates_left = done // accum, remaining // accum
            c["lr_schedule"] = {"name": "cosine_decay",
                                "arguments": [lr_at(u, sched), max(1, updates_left), sched["arguments"][2]]}
            if u < sched.get("warmup", 0):   # crashed during warm-up: redo the rest of it
                c["lr_schedule"]["warmup"] = sched["warmup"] - u
                c["lr_schedule"]["warmup_init"] = lr_at(u, sched)
                c["lr_schedule"]["arguments"] = [sched["arguments"][0], max(1, updates_left - (sched["warmup"] - u)),
                                                 sched["arguments"][2]]
        shutil.copy(adapter_dir / "adapter_config.json", seg_dir / "adapter_config.json") \
            if (adapter_dir / "adapter_config.json").exists() else None
        run_cfg = HERE / f".resume_seg{segment}.yaml"
        run_cfg.write_text(yaml.safe_dump(c, sort_keys=False))
    raise SystemExit(f"[train_resumable] gave up after {args.max_restarts} restarts ({done}/{total} iterations)")


if __name__ == "__main__":
    main()
