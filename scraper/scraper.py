import os, re, json, datetime, requests
from bs4 import BeautifulSoup

SOURCE_URL = os.getenv("SOURCE_URL", "https://www.conectate.com.do/articulo/precio-combustible-republica-dominicana/")
ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

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

MONTHS_ES = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}

RE_WEEK = re.compile(
    r"semana\s+del\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:al\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+)?de\s+(\d{4})",
    re.IGNORECASE
)

RE_PRICE_IN_CELL = re.compile(r"RD\$\s*[\d\.,]+", re.IGNORECASE)
RE_NUMBER = re.compile(r"[\d\.,]+")

def parse_price(s: str) -> float:
    s = (s or "").strip()
    s = s.replace("RD$", "").replace("DOP", "").strip()
    s = s.replace(" ", "")
    s = s.rstrip(".,")  # evita "6.85."
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

def extract_latest_week_anchor(soup: BeautifulSoup):
    """Devuelve (p_element, start_date, end_date) del ÚLTIMO párrafo 'semana del ...'."""
    latest = None
    for p in soup.select("p"):
        txt = p.get_text(" ", strip=True)
        m = RE_WEEK.search(txt)
        if m:
            d1, mon1, d2, mon2, yr = m.groups()
            start = parse_spanish_date(d1, mon1, yr)
            end = parse_spanish_date(d2, mon2 if mon2 else mon1, yr) if d2 else start
            latest = (p, start, end)
    if latest:
        return latest
    return (None, None, None)

def parse_table_generic(tbl: BeautifulSoup):
    """
    Lee una tabla sin depender de <thead>.
    Heurística:
      - Encuentra la columna de PRECIO como la que contenga "RD$" o un número con ,/. en la mayoría de filas.
      - La columna de NOMBRE será la primera celda no numérica/monetaria.
    """
    # Construir filas (excluir filas de encabezado si tienen <th>)
    rows = []
    for tr in tbl.find_all("tr"):
        # si toda la fila es th, sáltala
        if tr.find_all("th") and not tr.find_all("td"):
            continue
        cells = [c.get_text(" ", strip=True) for c in tr.find_all(["td","th"])]
        if cells and len(cells) >= 2:
            rows.append(cells)

    if not rows:
        return []

    # Inferir índice de precio
    n_cols = max(len(r) for r in rows)
    score_price = [0]*n_cols
    for r in rows:
        for i, c in enumerate(r):
            if i >= n_cols: continue
            if RE_PRICE_IN_CELL.search(c):
                score_price[i] += 2
            # Si parece número con separadores, suma 1 (pero evita nombres)
            elif RE_NUMBER.fullmatch(c.replace(" ", "")):
                score_price[i] += 1
    # columna candidata de precio:
    col_price = max(range(n_cols), key=lambda i: score_price[i])

    items = []
    for r in rows:
        # asegurar longitud
        if len(r) <= col_price: 
            continue
        price_text = r[col_price]
        # label = primera celda que no sea la de precio y que tenga algo parecido a nombre
        label = None
        for i, c in enumerate(r):
            if i == col_price: 
                continue
            # descartar celdas muy numéricas
            if RE_PRICE_IN_CELL.search(c):
                continue
            if RE_NUMBER.fullmatch(c.replace(" ", "")):  # ej "227.70"
                continue
            if c.strip():
                label = c.strip()
                break

        if not label:
            continue

        try:
            price = parse_price(price_text)
        except Exception:
            continue

        # variación (si hay otra celda con "sube/baja/mantiene")
        change_type, change_amount = None, None
        for c in r:
            low = c.lower()
            if "mantien" in low:
                change_type = "same"; break
            if "sube" in low:
                change_type = "up"
                m2 = RE_NUMBER.search(c)
                if m2:
                    try: change_amount = parse_price(m2.group(0))
                    except: pass
                break
            if "baja" in low:
                change_type = "down"
                m2 = RE_NUMBER.search(c)
                if m2:
                    try: change_amount = parse_price(m2.group(0))
                    except: pass
                break

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

def parse_table_after_anchor(anchor_p: BeautifulSoup):
    """
    Busca la PRIMERA tabla después del párrafo ancla;
    si no sirve (0 items), intenta con la SIGUIENTE tabla inmediata.
    """
    if anchor_p is None:
        return []
    # primera candidata
    t1 = anchor_p.find_next("table")
    candidates = []
    if t1: candidates.append(t1)
    # algunos sitios usan figure.wp-block-table
    w1 = anchor_p.find_next(lambda tag: tag.name in ("figure","div") and "wp-block-table" in (tag.get("class") or []))
    if w1:
        t_in = w1.find("table")
        if t_in and t_in not in candidates:
            candidates.append(t_in)
    # siguiente tabla por si acaso
    if t1:
        t2 = t1.find_next("table")
        if t2 and t2 not in candidates:
            candidates.append(t2)

    for tbl in candidates:
        items = parse_table_generic(tbl)
        if items:
            return items
    return []

def fetch():
    r = requests.get(SOURCE_URL, timeout=30, headers={
        "User-Agent": "CombustiblesRDBot/1.0 (+contacto: tu-email@dominio.do)"
    })
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # 1) Encontrar la semana vigente (ancla + fechas)
    anchor_p, start, end = extract_latest_week_anchor(soup)

    # 2) Solo TABLA (sin fallback a párrafos)
    items = parse_table_after_anchor(anchor_p)

    payload = {
        "source": SOURCE_URL,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {
            "start_date": start.isoformat() if start else None,
            "end_date": end.isoformat() if end else None
        },
        "currency": "DOP",
        "items": items
    }

    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

    with open(os.path.join(OUT_DIR, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    stamp = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    with open(os.path.join(HIST_DIR, f"{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    fetch()
