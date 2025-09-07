import os, json, glob, datetime

ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(DATA_DIR, "history")
OUT_PATH = os.path.join(DATA_DIR, "trend.json")

def load_history():
    entries = []
    for path in sorted(glob.glob(os.path.join(HIST_DIR, "*.json"))):
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
            date = doc.get("week", {}).get("end_date") or os.path.splitext(os.path.basename(path))[0]
            items = doc.get("items", [])
            entries.append((date, items))
        except Exception as e:
            print(f"[warn] no pude leer {path}: {e}")
    return entries

def build_trend(entries):
    trend = {}
    for date, items in entries:
        if not date: 
            continue
        for it in items:
            key = it.get("key")
            price = it.get("price_dop")
            if key is None or price is None:
                continue
            trend.setdefault(key, []).append({"date": date, "price_dop": price})

    for key in trend:
        trend[key].sort(key=lambda x: x["date"])

    trend_with_delta = {}
    for key, series in trend.items():
        out, prev = [], None
        for row in series:
            cur = dict(row)
            if prev is not None:
                d = cur["price_dop"] - prev["price_dop"]
                cur["delta_abs"] = round(d, 4)
                cur["delta_pct"] = round((d / prev["price_dop"] * 100.0), 4) if prev["price_dop"] else None
            else:
                cur["delta_abs"] = None
                cur["delta_pct"] = None
            out.append(cur); prev = row
        trend_with_delta[key] = out
    return trend_with_delta

def main():
    entries = load_history()
    trend = build_trend(entries)
    out = {
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "currency": "DOP",
        "series": trend,
        "keys": sorted(trend.keys())
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"trend.json escrito en {OUT_PATH} con {len(out['keys'])} series.")

if __name__ == "__main__":
    main()
