# scraper/micm_scraper.py
import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pdfplumber
from dateutil import parser as dtp

# Página del MICM que lista los avisos semanales (PDFs)
MICM_LIST_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

# === Normalización de claves ===
FUEL_KEYS = {
    "gasolina premium": "gasolina_premium",
    "gasolina regular": "gasolina_regular",
    "gasoil regular": "gasoil_regular",
    "gasoil óptimo": "gasoil_optimo",
    "gasoil optimo": "gasoil_optimo",
    "avtur": "avtur",
    "kerosene": "kerosene",
    "fuel oil #6": "fueloil_6",
    "fueloil #6": "fueloil_6",
    "fuel oil 1%s": "fueloil_1s",
    "fueloil 1%s": "fueloil_1s",
    "gas licuado de petróleo": "glp",
    "gas licuado de petroleo": "glp",
    "gas licuado de petróleo (glp)": "glp",
    "gas licuado de petroleo (glp)": "glp",
    "gas natural": "gas_natural",
}

LABEL_SYNONYMS = {
    "gasolina premium": "Gasolina Premium",
    "gasolina regular": "Gasolina Regular",
    "gasoil regular": "Gasoil Regular",
    "gasoil óptimo": "Gasoil Óptimo",
    "gasoil optimo": "Gasoil Óptimo",
    "avtur": "Avtur",
    "kerosene": "Kerosene",
    "fuel oil #6": "Fuel Oil #6",
    "fueloil #6": "Fuel Oil #6",
    "fuel oil 1%s": "Fuel Oil 1%S",
    "fueloil 1%s": "Fuel Oil 1%S",
    "gas licuado de petróleo": "Gas Licuado de Petróleo",
    "gas licuado de petroleo": "Gas Licuado de Petróleo",
    "gas natural": "Gas Natural",
}

TABLE_HEADER_HINTS = [
    "precio al público",
    "precio al publico",
    "precio rd$ por galón",
    "precio rd$ por galon",
    "precio rd$ por m³",
    "precio rd$ por m3",
    "precio al público rd$",
    "precio al publico rd$",
]

DEC_RE = re.compile(r"([\d\.\,]+)")

# === Fechas (semana del ... al ...) ===
RANGE_PATTERNS = [
    # "Semana del 30 de agosto al 5 de septiembre de 2025"
    re.compile(
        r"(?:semana\s+)?del\s+(\d{1,2}\s+de\s+[a-záéíóú]+)\s+(?:al|-)\s+(\d{1,2}\s+de\s+[a-záéíóú]+)\s+de\s+(\d{4})",
        re.IGNORECASE
    ),
    # "Semana del 6 al 12 de septiembre de 2025"
    re.compile(
        r"(?:semana\s+)?del\s+(\d{1,2})\s+(?:al|-)\s+(\d{1,2}\s+de\s+[a-záéíóú]+)\s+de\s+(\d{4})",
        re.IGNORECASE
    ),
    # "del 06 al 12/09/2025"
    re.compile(
        r"(?:semana\s+)?del\s+(\d{1,2})\s+(?:al|-)\s+(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})",
        re.IGNORECASE
    ),
]

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

# ------------------------------------------------------------
# Utilidades
# ------------------------------------------------------------

def parse_num(s: str) -> float:
    s = s.strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    return float(s)

def norm_key(label: str) -> str:
    k = label.lower()
    k = re.sub(r"\s+", " ", k)
    for needle, norm in FUEL_KEYS.items():
        if needle in k:
            return norm
    return re.sub(r"[^a-z0-9]+", "_", k).strip("_")

def normalize_label(raw: str) -> str | None:
    low = (raw or "").lower()
    # limpia notas entre paréntesis y espacios dobles
    low = re.sub(r"\(.*?\)", "", low)
    low = re.sub(r"\s+", " ", low).strip()

    for k, v in LABEL_SYNONYMS.items():
        if k in low:
            return v
    # tolerancias
    if "fuel" in low and "oil" in low and "1" in low and "s" in low:
        return "Fuel Oil 1%S"
    if "fuel" in low and "oil" in low and "#6" in low:
        return "Fuel Oil #6"
    return None

def get_latest_pdf_url_and_text():
    r = requests.get(MICM_LIST_URL, timeout=30, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.lower().endswith(".pdf"):
            url = urljoin(MICM_LIST_URL, href)
            txt = (a.get_text(" ", strip=True) or "")
            score = 0
            low = txt.lower()
            if "aviso" in low or "combustible" in low or "precio" in low:
                score += 5
            if "semana" in low:
                score += 1
            candidates.append((score, url, txt))
    if not candidates:
        raise RuntimeError("No se encontró PDF en el listado del MICM")
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1], candidates[0][2]

def extract_text_from_pdf(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        pages = [p.extract_text() or "" for p in pdf.pages]
    return re.sub(r"[ \t]+", " ", "\n".join(pages))

# ---------- Tablas (precio al público) ----------

def extract_tables_from_pdf_bytes(pdf_bytes: bytes):
    tables = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            try:
                for tbl in p.extract_tables() or []:
                    norm = [[(c or "").strip() for c in row]
                            for row in tbl
                            if any(c and str(c).strip() for c in row)]
                    if norm:
                        tables.append(norm)
            except Exception:
                continue
    return tables

def find_public_price_column(header_row: list[str]) -> int | None:
    for idx, cell in enumerate(header_row):
        low = (cell or "").lower()
        if any(hint in low for hint in TABLE_HEADER_HINTS):
            return idx
    return None

def parse_prices_from_tables(pdf_bytes: bytes) -> list[dict]:
    tables = extract_tables_from_pdf_bytes(pdf_bytes)
    items = []

    for tbl in tables:
        if not tbl:
            continue

        header = tbl[0]
        pub_col = find_public_price_column(header)
        if pub_col is None:
            if len(tbl) > 1 and find_public_price_column(tbl[1]) is not None:
                header = tbl[1]
                pub_col = find_public_price_column(header)
                body = tbl[2:]
            else:
                continue
        else:
            body = tbl[1:]

        # Heurística: columna de producto
        label_col = 0
        for i, cell in enumerate(header):
            low = (cell or "").lower()
            if "producto" in low or "combustible" in low or "concepto" in low:
                label_col = i
                break

        for row in body:
            if label_col >= len(row) or pub_col >= len(row):
                continue
            raw_label = (row[label_col] or "").strip()
            raw_price = (row[pub_col] or "").strip()
            if not raw_label or not raw_price:
                continue

            label_norm = normalize_label(raw_label)
            if not label_norm:
                continue

            m = DEC_RE.search(raw_price)
            if not m:
                continue
            price = parse_num(m.group(1))
            unit = "m3" if "natural" in label_norm.lower() else "galon"

            items.append({
                "label": label_norm,
                "key": norm_key(label_norm),
                "price_dop": round(price, 2),
                "unit": unit,
                "change": None
            })

        if items:
            return items

    return []

# ---------- Texto (bloque "Precio al público") ----------

def parse_prices_from_text(pdf_text: str) -> list[dict]:
    text = pdf_text
    low = text.lower()

    start_idx = low.find("precio al público")
    if start_idx == -1:
        start_idx = low.find("precio al publico")
    if start_idx == -1:
        return []  # no arriesgar a capturar paridad

    block = text[start_idx : start_idx + 4000]

    items = []
    for line in block.splitlines():
        line = line.strip()
        if not line:
            continue
        m = DEC_RE.search(line)
        if not m:
            continue

        label_guess = normalize_label(line.split(m.group(0))[0])
        if not label_guess:
            continue

        price = parse_num(m.group(1))
        unit = "m3" if "natural" in label_guess.lower() else "galon"
        items.append({
            "label": label_guess,
            "key": norm_key(label_guess),
            "price_dop": round(price, 2),
            "unit": unit,
            "change": None
        })

    # dedup por key
    dedup, seen = [], set()
    for it in items:
        if it["key"] in seen:
            continue
        seen.add(it["key"])
        dedup.append(it)
    return dedup

# ---------- Semana (rango de fechas) ----------

def _parse_spanish_date(fragment: str, default_year: int | None = None, default_month: int | None = None):
    frag = fragment.strip().lower()
    m = re.match(r"(\d{1,2})\s+de\s+([a-záéíóú]+)", frag, flags=re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month_name = (m.group(2)
                      .replace("á","a").replace("é","e")
                      .replace("í","i").replace("ó","o")
                      .replace("ú","u"))
        month = SPANISH_MONTHS.get(month_name)
        if month and default_year:
            return datetime.date(default_year, month, day)
        return None
    m2 = re.match(r"^(\d{1,2})$", frag)
    if m2 and default_year and default_month:
        return datetime.date(default_year, default_month, int(m2.group(1)))
    return None

def parse_week_from_text(text: str):
    for pat in RANGE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue

        if pat is RANGE_PATTERNS[0]:
            a, b, year = m.groups()
            year = int(year)
            end_date = _parse_spanish_date(b, default_year=year)
            start_date = _parse_spanish_date(a, default_year=year, default_month=end_date.month if end_date else None)
            if start_date and end_date:
                return start_date.isoformat(), end_date.isoformat()

        elif pat is RANGE_PATTERNS[1]:
            a_day, b_full, year = m.groups()
            year = int(year)
            end_date = _parse_spanish_date(b_full, default_year=year)
            start_date = _parse_spanish_date(a_day, default_year=year, default_month=end_date.month if end_date else None)
            if start_date and end_date:
                return start_date.isoformat(), end_date.isoformat()

        else:
            a_day, b_day, b_month, year = m.groups()
            year = int(year); b_day = int(b_day); b_month = int(b_month)
            end_date = datetime.date(year, b_month, b_day)
            start_date = _parse_spanish_date(a_day, default_year=year, default_month=b_month)
            if start_date and end_date:
                return start_date.isoformat(), end_date.isoformat()

    return None, None

def parse_week(text_pdf: str, hint_text: str | None = None, pdf_url: str | None = None):
    s, e = parse_week_from_text(text_pdf)
    if s and e:
        return s, e
    if hint_text:
        s, e = parse_week_from_text(hint_text)
        if s and e:
            return s, e
    if pdf_url:
        fname = os.path.basename(pdf_url)
        clean = re.sub(r"[_\-\.]+", " ", fname)
        s, e = parse_week_from_text(clean)
        if s and e:
            return s, e
    return None, None

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------

def main():
    pdf_url, link_text = get_latest_pdf_url_and_text()

    # descarga bytes para tablas y texto para week/backup
    r = requests.get(pdf_url, timeout=60, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    pdf_bytes = r.content

    pdf_text = extract_text_from_pdf(pdf_url)

    # 1) Prioriza TABLAS con "Precio al público"
    items = parse_prices_from_tables(pdf_bytes)
    # 2) Si falla, usa el bloque textual "Precio al público"
    if not items:
        items = parse_prices_from_text(pdf_text)
    # 3) Si aun no hay items, NO publiques paridad
    if not items:
        items = []

    start_date, end_date = parse_week(pdf_text, hint_text=link_text, pdf_url=pdf_url)

    payload = {
        "source": pdf_url,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {"start_date": start_date, "end_date": end_date},
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
    main()
