import os, re, json, datetime, requests
from bs4 import BeautifulSoup

SOURCE_URL = os.getenv("SOURCE_URL", "https://www.conectate.com.do/articulo/precio-combustible-republica-dominicana/")
ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

# Normalización de combustibles
FUEL_KEYS = {
    "gasolina premium": "gasolina_premium",
    "gasolina regular": "gasolina_regular",
    "gasoil regular": "gasoil_regular",
    "gasoil óptimo": "gasoil_optimo",
    "gasoil optimo": "gasoil_optimo",
    "avtur": "avtur",
    "kerosene": "kerosene",
    "fuel oíl #6": "fueloil_6",
    "fueloil #6": "fueloil_6",
    "fuel oil #6": "fueloil_6",
    "fueloil 1%s": "fueloil_1s",
    "fuel oil 1s": "fueloil_1s",
    "gas licuado de petróleo (glp)": "glp",
    "gas licuado de petroleo (glp)": "glp",
    "gas licuado de petróleo": "glp",
    "gas licuado de petroleo": "glp",
    "glp": "glp",
    "gas natural": "gas_natural"
}

# Encabezado de semana: “semana del 30 de agosto al 5 de septiembre de 2025”
MONTHS_ES = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}
RE_WEEK = re.compile(
    r"semana\s+del\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:al\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+)?de\s+(\d{4})",
    re.IGNORECASE
)

RE_NUMBER = re.compile(r"[\d\.,]+", re.IGNORECASE)

def parse_price(s: str) -> float:
    s = (s or "").strip()
    s = s.replace("RD$", "").replace("DOP", "").strip()
    s = s.replace(" ", "")
    s = s.rstrip(".,")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    return float(s)

def norm_key(label: str) -> str:
    k = label.lower().strip()
    k = re.sub(r"\s+", " ", k)
    for needle, norm in FUEL_KEYS.items():
        if needle in k:
            return norm
    k = re.sub(r"[^a-z0-9]+", "_", k)
    return k.strip("_")

def parse_spanish_date(day: str, month_name: str, year: str):
    d = int(day)
    m = MONTHS_ES.get(month_name.lower())
    y = int(year)
    if not m:
        return None
    return datetime.date(y, m, d)

def extract_latest_week_dates(soup: BeautifulSoup):
    """Busca la ÚLTIMA mención de 'semana del...' y devuelve (start_date, end_date)."""
    start = end = None
    for p in soup.select("p"):
        txt = p.get_text(" ", strip=True)
        m = RE_WEEK.search(txt)
        if m:
            d1, mon1, d2, mon2, yr = m.groups()
            s = parse_spanish_date(d1, mon1, yr)
            e = parse_spanish_date(d2 or d1, (mon2 or mon1), yr)
            if s or e:
                start, end = s, e
    return start, end

def extract_api_url(soup: BeautifulSoup) -> str | None:
    tbl = soup.select_one("table.widget_fuel_price_table")
    if not tbl:
        return None
    return tbl.get("data-url")

def pick_first_value(d: dict, keys: list[str]) -> str | None:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    # admite variaciones por mayúsculas/minúsculas
    lower = {k.lower(): v for k, v in d.items()}
    for k in keys:
        lk = k.lower()
        if lk in lower and lower[lk] not in (None, ""):
            return lower[lk]
    return None

def parse_api_payload(obj) -> list[dict]:
    """
    Convierte el JSON del endpoint en la lista de items estándar.
    Soporta formas comunes: {"data":[...]}, {"rows":[...]}, o lista directa.
    Para cada fila, intenta detectar:
      - label: ['combustible','fuel','name','label','producto','product']
      - price: ['precio','price','value','price_dop']
      - change: ['variacion','variation','change','nota','note']
      - unit:   ['unidad','unit']
    """
    # 1) normalizar filas
    if isinstance(obj, dict):
        rows = obj.get("data") or obj.get("rows") or obj.get("items") or obj.get("result") or []
        if isinstance(rows, dict):  # a veces viene { key: {...}, ... }
            rows = list(rows.values())
    elif isinstance(obj, list):
        rows = obj
    else:
        rows = []

    items = []
    for r in rows:
        if isinstance(r, dict):
            label = pick_first_value(r, ["combustible","fuel","name","label","producto","product"])
            price_text = pick_first_value(r, ["precio","price","value","price_dop","rd","rd$"])
            note = pick_first_value(r, ["variacion","variation","change","nota","note","detalle","detalles"])

            # si no hay label o price claro, probar si el dict solo tiene 2-3 valores útiles
            if not label:
                # busca primer campo de texto no-numérico razonable
                for k, v in r.items():
                    if isinstance(v, str) and v and not RE_NUMBER.fullmatch(v.replace(" ", "")):
                        label = v; break

            if not price_text:
                # busca primer campo que luzca como monto "RD$ ..."
                for k, v in r.items():
                    if isinstance(v, str) and ("RD$" in v or RE_NUMBER.fullmatch(v.replace(" ", ""))):
                        price_text = v; break

            if not label or not price_text:
                continue

            try:
                price = parse_price(price_text)
            except Exception:
                continue

            # unidad (si la proveen), si no, asumimos galón
            unit = pick_first_value(r, ["unidad","unit"]) or "galon"

            change_type = None
            change_amount = None
            if isinstance(note, str) and note:
                low = note.lower()
                if "mantien" in low:
                    change_type = "same"
                elif "sube" in low:
                    change_type = "up"
                    m2 = RE_NUMBER.search(note)
                    if m2:
                        try: change_amount = parse_price(m2.group(0))
                        except Exception: pass
                elif "baja" in low:
                    change_type = "down"
                    m2 = RE_NUMBER.search(note)
                    if m2:
                        try: change_amount = parse_price(m2.group(0))
                        except Exception: pass

            items.append({
                "label": str(label).strip(),
                "key": norm_key(str(label)),
                "price_dop": price,
                "unit": unit,
                "change": {
                    "type": change_type,
                    "amount_dop": change_amount
                } if change_type else None
            })

        elif isinstance(r, (list, tuple)) and len(r) >= 2:
            # Formato tabular simple: [label, price, (variation?)]
            label = str(r[0]).strip()
            price_text = str(r[1]).strip()
            note = str(r[2]).strip() if len(r) >= 3 else None
            try:
                price = parse_price(price_text)
            except Exception:
                continue
            change_type = change_amount = None
            if note:
                low = note.lower()
                if "mantien" in low:
                    change_type = "same"
                elif "sube" in low:
                    change_type = "up"
                    m2 = RE_NUMBER.search(note)
                    if m2:
                        try: change_amount = parse_price(m2.group(0))
                        except Exception: pass
                elif "baja" in low:
                    change_type = "down"
                    m2 = RE_NUMBER.search(note)
                    if m2:
                        try: change_amount = parse_price(m2.group(0))
                        except Exception: pass
            items.append({
                "label": label,
                "key": norm_key(label),
                "price_dop": price,
                "unit": "galon",
                "change": {
                    "type": change_type,
                    "amount_dop": change_amount
                } if change_type else None
            })

    return items

def fetch():
    # 1) Cargar HTML para sacar fechas y URL del API
    r = requests.get(SOURCE_URL, timeout=30, headers={
        "User-Agent": "CombustiblesRDBot/1.0 (+contacto: tu-email@dominio.do)"
    })
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    start_date, end_date = extract_latest_week_dates(soup)

    api_url = extract_api_url(soup)
    if not api_url:
        raise RuntimeError("No se encontró el atributo data-url en la tabla widget_fuel_price_table.")

    # 2) Llamar el API de la tabla
    r2 = requests.get(api_url, timeout=30, headers={
        "User-Agent": "CombustiblesRDBot/1.0 (+contacto: tu-email@dominio.do)",
        "Accept": "application/json"
    })
    r2.raise_for_status()

    try:
        payload_in = r2.json()
    except Exception:
        # si devolviera texto, intenta JSON a mano
        payload_in = json.loads(r2.text)

    items = parse_api_payload(payload_in)

    payload_out = {
        "source": SOURCE_URL,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat() if end_date else None
        },
        "currency": "DOP",
        "items": items
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

    with open(os.path.join(OUT_DIR, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(payload_out, f, ensure_ascii=False, indent=2)

    stamp = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    with open(os.path.join(HIST_DIR, f"{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump(payload_out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    fetch()
