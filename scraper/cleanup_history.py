"""
One-shot cleanup for data/history/*.json files corrupted by the prior
parser bug (which captured the VARIACION column instead of the price
column, planting values like RD$23.15 for Avtur and RD$25.00 for
Kerosene where the real prices were 277 and 314).

Strategy:
  * Walk every history snapshot.
  * For each fuel item, drop it when its `price_dop` is below
    SANITY_FLOOR_DOP — no liquid combustible nor cylinder in the MICM
    catalog has ever priced below that. The dropped items become
    invisible to the trend builder, so the chart stops showing the
    nonsense dips and the "Mínimo" stat reflects reality.

Then re-run build_trend.py / build_trend_min.py to rebuild trend.json
and trend_min.json off the cleaned snapshots.
"""

import json
import os
import sys

# Below this floor we're certain the parser was reading the VARIACION
# column. Real fuel prices in DOP/gal are all >= ~130 (GLP). 50 is a
# comfortable safety margin that won't ever discard a legit row.
SANITY_FLOOR_DOP = 50.0

ROOT = os.path.dirname(__file__)
HIST_DIR = os.path.abspath(os.path.join(ROOT, "..", "data", "history"))


def clean_file(path: str) -> tuple[int, int]:
    """Returns (dropped, kept) for stats."""
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    items = payload.get("items") or []
    cleaned = []
    dropped = 0
    for it in items:
        price = it.get("price_dop")
        if isinstance(price, (int, float)) and price < SANITY_FLOOR_DOP:
            dropped += 1
            continue
        cleaned.append(it)
    if dropped == 0:
        return 0, len(items)
    payload["items"] = cleaned
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return dropped, len(cleaned)


def main() -> int:
    if not os.path.isdir(HIST_DIR):
        print(f"history dir not found: {HIST_DIR}", file=sys.stderr)
        return 1

    total_dropped = 0
    total_files_touched = 0
    files = sorted(f for f in os.listdir(HIST_DIR) if f.endswith(".json"))
    for fn in files:
        path = os.path.join(HIST_DIR, fn)
        dropped, kept = clean_file(path)
        if dropped:
            total_dropped += dropped
            total_files_touched += 1
            print(f"  {fn}: dropped {dropped}, kept {kept}")

    print(
        f"\nDone. {total_files_touched}/{len(files)} files touched; "
        f"{total_dropped} corrupt rows removed."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
