import os, re, json, datetime, requests
from bs4 import BeautifulSoup
from dateutil import parser as dtp

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
    "fueloil #6": "fueloil_6",
    "fueloil 1%s": "fueloil_1s",
    "gas licuado de petróleo (glp)": "glp",
    "gas licuado de petroleo (glp)": "glp",
    "gas natural": "gas_natural"
}

PRICE_RE = re.compile(
    r"^\s*([^:]+):\s*RD\$\s*([\d\.,]+)\s+por\s+(gal[oó]n|m³|m3)\s*;?\s*(mantiene su precio|sube\s+RD\$\s*[\d\.,]+|baja\s+RD\$\s*[\d\.,]+)?",
    re.IGNORECASE
)

RANGE_RE = re.compile(
    r"semana\s+del\s+([^,]+?)(?:,|\s+)(?:al\s+([^,]+?))?\s+de\s+(\d{4})",
    re.IGNORECASE
)

def norm_key(label: str) -> str:
    k = label.lower().strip()
    k = re.sub(r"\s+", " ", k)
    for needle, norm in FUEL_KEYS.items():
        if needle in k:
            return norm
    k = re.sub(r"[^a-z0-9]+", "_", k)
    return k.strip("_")

def parse_date_range(text_block: str):
    m = RANGE_RE.search(text_block.replace("\n", " "))
    if not m:
        return None, None
    start_str, end_str, year_str = m.groups()
    year = int(year_str)
    try:
        start = dtp.parse(f"{start_str.strip()} {year}", dayfirst=True, fuzzy=True)
    except Exception:
        start = None
    try:
        if end_str:
            if start:
                end = dtp.parse(f"{end_str.strip()} {year}", dayfirst=True, fuzzy=True, default=start)
            else:
                end = dtp.parse(f"{end_str.strip()} {year}", dayfirst=True, fuzzy=True)
        else:
            end = start
    except Exception:
        end = None
    return start, end

def fetch():
    r = requests.get(SOURCE_URL, timeout=30, headers={
        "User-Agent": "CombustiblesRDBot/1.0 (+contacto: tu-email@dominio.do)"
    })
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    ps = [p.get_text(" ", strip=True) for p in soup.select("p")]
    full_text = " \n".join(ps)
    start, end = parse_date_range(full_text)

    items = []
    for p in ps:
        m = PRICE_RE.match(p)
        if not m:
            continue
        label, price_str, unit, note = m.groups()
        price = float(price_str.replace(".", "").replace(",", ".")) if price_str else None
        unit_norm = {"galón": "galon", "galon": "galon", "m³": "m3", "m3": "m3"}.get(unit.lower(), unit.lower())
        change_type, change_amount = None, None
        if note:
            low = note.lower()
            if "mantiene" in low:
                change_type = "same"
            elif "sube" in low:
                change_type = "up"
                m2 = re.search(r"([\d\.,]+)", low)
                if m2: change_amount = float(m2.group(1).replace(".", "").replace(",", "."))
            elif "baja" in low:
                change_type = "down"
                m2 = re.search(r"([\d\.,]+)", low)
                if m2: change_amount = float(m2.group(1).replace(".", "").replace(",", "."))
        items.append({
            "label": label.strip(),
            "key": norm_key(label),
            "price_dop": price,
            "unit": unit_norm,
            "change": {
                "type": change_type,
                "amount_dop": change_amount
            } if note else None
        })

    payload = {
        "source": SOURCE_URL,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {
            "start_date": start.date().isoformat() if start else None,
            "end_date": end.date().isoformat() if end else None
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
