"""trend_todo.json: toda la serie desde 2010 para el rango «Todo» de la app.

Junta el histórico viejo (data/historico/*.json, 2010 → ago-2021) con el
historial semanal (data/history). Para que pese poco deja un punto cuando
cambia el precio y, si no cambia, uno cada 4 semanas, más el último.
La app solo lo baja al tocar «Todo»; los demás rangos usan trend_min.json.
"""
import os, json, glob, datetime

ROOT = os.path.dirname(__file__)
DATA_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
OUT_PATH = os.path.join(DATA_DIR, "trend_todo.json")


def main():
    puntos = {}  # clave -> {fecha: precio}
    for p in sorted(glob.glob(os.path.join(DATA_DIR, "historico", "*.json"))):
        with open(p, encoding="utf-8") as f:
            doc = json.load(f)
        for fecha, precios in doc.get("semanas", {}).items():
            for clave, precio in precios.items():
                puntos.setdefault(clave, {})[fecha] = precio
    for p in sorted(glob.glob(os.path.join(DATA_DIR, "history", "*.json"))):
        with open(p, encoding="utf-8") as f:
            doc = json.load(f)
        fecha = os.path.splitext(os.path.basename(p))[0]
        for it in doc.get("items", []):
            if it.get("key") and it.get("price_dop") is not None:
                puntos.setdefault(it["key"], {})[fecha] = round(float(it["price_dop"]), 2)

    series = {}
    for clave, por_fecha in puntos.items():
        orden = sorted(por_fecha.items())
        ligera = []
        for fecha, precio in orden:
            if ligera:
                ant_f, ant_p = ligera[-1]["date"], ligera[-1]["price_dop"]
                dias = (datetime.date.fromisoformat(fecha) - datetime.date.fromisoformat(ant_f)).days
                if precio == ant_p and dias < 28:
                    continue
            ligera.append({"date": fecha, "price_dop": precio})
        if orden and ligera[-1]["date"] != orden[-1][0]:
            ligera.append({"date": orden[-1][0], "price_dop": orden[-1][1]})
        series[clave] = ligera

    out = {
        "updated_at_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "currency": "DOP",
        "series": series,
        "keys": sorted(series),
    }
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, separators=(",", ":"))
    print(f"trend_todo.json escrito con {len(series)} series.")


if __name__ == "__main__":
    main()
