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

PRICE_RE = re.compile(
    r"^\s*([^:]+):\s*RD\$\s*([\d\.,]+)\s+(?:por\s+(gal[oó]n|m³|m3))?\s*;?\s*(mantiene su precio|sube\s+RD\$\s*[\d\.,]+|baja\s+RD\$\s*[\d\.,]+)?",
    re.IGNORECASE
)

RANGE_RE = re.compile(
    r"semana\s+del\s+([^,]+?)(?:,|\s+)(?:al\s+([^,]+?))?\s+de\s+(\d{4})",
    re.IGNORECASE
)

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

def find_latest_week_paragraph(soup: BeautifulSoup):
    """Devuelve (p_element, start_date, end_date) del ÚLTIMO párrafo que menciona 'semana del'."""
    latest = None
    for p in soup.select("p"):
        txt = p.get_text(" ", strip=True)
        if RANGE_RE.search(txt):
            start, end = parse_date_range(txt)
            if start or end:
                latest = (p, start, end)
    return latest if latest else (None, None, None)

# ---------- PARSEAR TABLA DESPUÉS DEL PÁRRAFO DE LA SEMANA ACTUAL ---------- #
def parse_table_after(anchor_p: BeautifulSoup):
    """
    Toma la PRIMERA <table> DESPUÉS del párrafo que contiene la semana actual.
    Interpreta columnas tipo: 'Combustible' / 'Precio' / 'Variación' (si existe).
    """
    if anchor_p is None:
        return None

    tbl = anchor_p.find_next("table")
    if not tbl:
        # Algunos sitios envuelven la tabla en <figure class="wp-block-table"> -> table adentro
        wrapper = anchor_p.find_next(lambda tag: tag.name in ("figure","div") and "table" in tag.get("class", []) or "wp-block-table" in tag.get("class", []))
        if wrapper:
            tbl = wrapper.find("table")
    if not tbl:
        return None

    # headers
    headers = [th.get_text(" ", strip=True).lower() for th in tbl.select("thead th")]
    if not headers:
        first_ths = [th.get_text(" ", strip=True).lower() for th in tbl.select("tr th")]
        headers = first_ths

    if not headers or len(headers) < 2:
        return None
    if not any("combust" in h for h in headers) or not any("precio" in h for h in headers):
        return None

    # localizar columnas
    try:
        col_name = next(i for i, h in enumerate(headers) if "combust" in h)
    except StopIteration:
        return None
    try:
        col_price = next(i for i, h in enumerate(headers) if "precio" in h)
    except StopIteration:
        return None

    col_var = None
    for i, h in enumerate(headers):
        if any(k in h for k in ["variación", "variacion", "cambio", "diferencia"]):
            col_var = i; break

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
            "unit": "galon",
            "change": {
                "type": change_type,
                "amount_dop": change_amount
            } if change_type else None
        })

    return items or None

# ------------------------ FALLBACK: PÁRRAFOS ANCLADOS ---------------------- #
def parse_paragraphs_after(anchor_p: BeautifulSoup):
    if anchor_p is None:
        return []
    items = []
    # Recorremos los hermanos siguientes hasta que aparezca otro encabezado de semana
    node = anchor_p
    while True:
        node = node.find_next()  # siguiente nodo en DOM
        if node is None:
            break
        if node.name == "p":
            txt = node.get_text(" ", strip=True)
            if RANGE_RE.search(txt) and node is not anchor_p:
                break  # alcanzamos otra semana -> cortamos
            m = PRICE_RE.match(txt)
            if m:
                label, price_str, unit, note = m.groups()
                try:
                    price = parse_price(price_str) if price_str else None
                except Exception:
                    continue
                unit_norm = {"galón": "galon", "galon": "galon", "m³": "m3", "m3": "m3"}.get((unit or "").lower(), "galon")

                change_type, change_amount = None, None
                if note:
                    low = note.lower()
                    if "mantiene" in low:
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
                    "unit": unit_norm,
                    "change": {
                        "type": change_type,
                        "amount_dop": change_amount
                    } if change_type else None
                })
        # Si encontramos otra tabla antes de precios en párrafos, igual rompemos (ya no es el bloque de texto)
        if node.name == "table":
            break
    return items

# --------------------------------- MAIN ----------------------------------- #
def fetch():
    r = requests.get(SOURCE_URL, timeout=30, headers={
        "User
