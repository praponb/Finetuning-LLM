"""v18 option B: the exact average of two LoRA adapters (no training).

A LoRA layer adds  scale * x @ lora_a @ lora_b  to the frozen weight. Stacking two rank-r adapters side by side
(lora_a along the rank axis, lora_b along its rank axis, each lora_b times its weight) gives one rank-2r adapter whose
update is exactly  w1 * delta1 + w2 * delta2.  With w1 = w2 = 0.5 that is the average of v15 and v17.

Both inputs must share num_layers, keys, scale and dropout (v15 and v17 do: rank 16, scale 20, last 8 blocks).
Run from sql2pyspark-mlx/:
    .venv/bin/python v18/average.py                    # v15 + v17 -> v18/adapters_avg (rank 32)
"""
import argparse
import json
import shutil
from pathlib import Path

import mlx.core as mx

HERE = Path(__file__).resolve().parent.parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--a", default="schema_v15/adapters")
    ap.add_argument("--b", default="complex_v17/adapters")
    ap.add_argument("--weight-a", type=float, default=0.5)
    ap.add_argument("--out", default="v18/adapters_avg")
    args = ap.parse_args()

    da, db, out = HERE / args.a, HERE / args.b, HERE / args.out
    ca = json.loads((da / "adapter_config.json").read_text())
    cb = json.loads((db / "adapter_config.json").read_text())
    for k in ("num_layers", "fine_tune_type", "model"):
        assert ca[k] == cb[k], f"{k} differs: {ca[k]} vs {cb[k]}"
    la, lb = ca["lora_parameters"], cb["lora_parameters"]
    assert la["scale"] == lb["scale"] and la.get("keys") == lb.get("keys"), "lora scale/keys differ"

    wa = mx.load(str(da / "adapters.safetensors"))
    wb = mx.load(str(db / "adapters.safetensors"))
    assert sorted(wa) == sorted(wb), "adapter keys differ"
    w1, w2 = args.weight_a, 1.0 - args.weight_a

    merged = {}
    for k in sorted(wa):
        if k.endswith("lora_a"):          # (in, r): concatenate along the rank axis
            merged[k] = mx.concatenate([wa[k], wb[k]], axis=-1)
        elif k.endswith("lora_b"):        # (r, out): concatenate along the rank axis, weighted
            merged[k] = mx.concatenate([w1 * wa[k], w2 * wb[k]], axis=0)
        else:
            raise SystemExit(f"unexpected key {k}")

    # exactness check on one layer: merged update == w1 * delta_a + w2 * delta_b
    k0 = next(k for k in sorted(wa) if k.endswith("lora_a"))[: -len("lora_a")]
    x = mx.random.normal((4, wa[k0 + "lora_a"].shape[0]))
    want = w1 * (x @ wa[k0 + "lora_a"] @ wa[k0 + "lora_b"]) + w2 * (x @ wb[k0 + "lora_a"] @ wb[k0 + "lora_b"])
    got = x @ merged[k0 + "lora_a"] @ merged[k0 + "lora_b"]
    err = mx.abs(got - want).max().item() / (mx.abs(want).max().item() + 1e-12)
    assert err < 1e-4, f"merge check failed (rel err {err})"

    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    mx.save_safetensors(str(out / "adapters.safetensors"), merged)
    cfg = dict(cb)
    cfg["adapter_path"] = "./" + args.out
    cfg["lora_parameters"] = dict(lb, rank=la["rank"] + lb["rank"])
    cfg["merged_from"] = {args.a: w1, args.b: w2}
    (out / "adapter_config.json").write_text(json.dumps(cfg, indent=4) + "\n")
    print(f"wrote {out} ({len(merged)} tensors, rank {cfg['lora_parameters']['rank']}, check rel err {err:.1e})")


if __name__ == "__main__":
    main()
