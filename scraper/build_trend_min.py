import os, json, glob, math, datetime

ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(DATA_DIR, "history")
OUT_PATH = os.path.join(DATA_DIR, "trend_min.json")

def load_history():
    rows = []
    for p in sorted(glob.glob(os.path.join(HIST_DIR, "*.json"))):
        with open(p, "r", encoding="utf-8") as f:
            doc = json.load(f)
        # Día de publicación (nombre del archivo). Antes se usaba week.end_date
        # cuando venía, y esas semanas salían 7 días tarde en la gráfica.
        date = os.path.splitext(os.path.basename(p))[0]
        for it in doc.get("items", []):
            key = it.get("key")
            price = it.get("price_dop")
            if key is None or price is None:
                continue
            rows.append((key, date, price))
    return rows

def main():
    trend = {}
    for key, date, price in load_history():
        trend.setdefault(key, []).append({"date": date, "price_dop": round(float(price), 2)})
    # ordena por fecha y aligera: hay un archivo por día con el mismo precio.
    # Se queda un punto cuando cambia el precio, al menos uno por semana y el
    # último (así 1M sigue teniendo puntos y el archivo pesa poco).
    for key in trend:
        trend[key].sort(key=lambda x: x["date"])
        ligera = []
        for punto in trend[key]:
            if ligera:
                ant = ligera[-1]
                dias = (datetime.date.fromisoformat(punto["date"]) - datetime.date.fromisoformat(ant["date"])).days
                if punto["price_dop"] == ant["price_dop"] and dias < 7:
                    continue
            ligera.append(punto)
        if trend[key] and ligera[-1] is not trend[key][-1]:
            ligera.append(trend[key][-1])
        trend[key] = ligera

    out = {
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "currency": "DOP",
        "series": trend,
        "keys": sorted(trend.keys())
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"trend_min.json escrito en {OUT_PATH} con {len(out['keys'])} series.")

if __name__ == "__main__":
    main()
