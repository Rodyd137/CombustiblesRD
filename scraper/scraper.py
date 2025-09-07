import os, re, json, datetime, requests
from bs4 import BeautifulSoup
from dateutil import parser as dtp

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

# Encabezado de semana: "para la semana del 30 de agosto al 6 de septiembre de 2025"
RANGE_RE = re.compile(
    r"semana\s+del\s+([^,]+?)(?:,|\s+)(?:al\s+([^,]+?))?\s+de\s+(\d{4})",
    re.IGNORECASE
)

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

def parse_date_range(text: str):
    m = RANGE_RE.search(text.replace("\n", " "))
    if not m:
        return None, None
    start_str, end_str, year_str = m.groups()
    yr = int(year_str)
    try:
        start = dtp.parse(f"{start_str.strip()} {yr}", dayfirst=True, fuzzy=True)
    except Exception:
        start = None
    try:
        if end_str:
            if start:
                end = dtp.parse(f"{end_str.strip()} {yr}", dayfirst=True, fuzzy=True, default=start)
            else:
                end = dtp.parse(f"{end_str.strip()} {yr}", dayfirst=True, fuzzy=True)
        else:
            end = start
    except Exception:
        end = None
    return start, end

def extract_latest_week(soup: BeautifulSoup):
    """Busca la última mención de 'semana del…' para sacar fechas."""
    ps = [p.get_text(" ", strip=True) for p in soup.select("p")]
    found = []
    for i, p in enumerate(ps):
        if RANGE_RE.search(p):
            start, end = parse_date_range(p)
            if start or end:
                found.append((i, start, end))
    if not found:
        return None, None
    return found[-1][1], found[-1][2]  # start, end

def parse_table(soup: BeautifulSoup):
    tables = soup.select("table")
    for tbl in tables:
        headers = [th.get_text(" ", strip=True).lower() for th in tbl.select("thead th")]
        if not headers:
            first_row_th = [th.get_text(" ", strip=True).lower() for th in tbl.select("tr th")]
            if first_row_th:
                headers = first_row_th

        if not headers or len(headers) < 2:
            continue
        if not any("combust" in h for h in headers) or not any("precio" in h for h in headers):
            continue

        try:
            col_name = next(i for i, h in enumerate(headers) if "combust" in h)
            col_price = next(i for i, h in enumerate(headers) if "precio" in h)
        except StopIteration:
            continue

        col_var = None
        for i, h in enumerate(headers):
            if any(k in h for k in ["variación", "variacion", "cambio", "diferencia"]):
                col_var = i
                break

        items = []
        for tr in tbl.select("tbody tr"):
            tds = [td.get_text(" ", strip=True) for td in tr.select("td")]
            if not tds or len(tds) <= max(col_price, col_name):
                continue

            label = tds[col_name]
            price_text = tds[col_price]
            note = tds[col_var] if (col_var is not None and col_var < len(tds)) else None

            try:
                price = parse_price(price_text)
            except Exception:
                continue

            change_type, change_amount = None, None
            if note:
                low = note.lower()
                if "mantien" in low:
                    change_type = "same"
                elif "sube" in low:
                    change_type = "up"
                    m2 = re.search(r"([\d\.,]+)", note)
                    if m2:
                        try: change_amount = parse_price(m2.group(1))
                        except Exception: pass
                elif "baja" in low:
                    change_type = "down"
                    m2 = re.search(r"([\d\.,]+)", note)
                    if m2:
                        try: change_amount = parse_price(m2.group(1))
                        except Exception: pass

            items.append({
                "label": label.strip(),
                "key": norm_key(label),
                "price_dop": price,
                "unit": "galon",   # tabla no trae unidad, asumimos galón
                "change": {
                    "type": change_type,
                    "amount_dop": change_amount
                } if change_type else None
            })

        if items:
            return items
    return None

def fetch():
    r = requests.get(SOURCE_URL, timeout=30, headers={
        "User-Agent": "CombustiblesRDBot/1.0 (+contacto: tu-email@dominio.do)"
    })
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    start, end = extract_latest_week(soup)
    items = parse_table(soup)

    payload = {
        "source": SOURCE_URL,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {
            "start_date": start.date().isoformat() if start else None,
            "end_date": end.date().isoformat() if end else None
        },
        "currency": "DOP",
        "items": items or []
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
