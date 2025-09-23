# scraper/micm_scraper.py
import os, re, io, json, datetime, requests, unicodedata
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pdfplumber

# Página del MICM que lista los avisos (PDFs)
MICM_LIST_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

# -------------------------
# Normalización de claves
# -------------------------
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

# -------------------------
# Fechas (semana del ... al ...)
# -------------------------
RANGE_PATTERNS = [
    re.compile(
        r"(?:semana\s+)?del\s+(\d{1,2}\s+de\s+[a-záéíóú]+)\s+(?:al|-)\s+(\d{1,2}\s+de\s+[a-záéíóú]+)\s+de\s+(\d{4})",
        re.IGNORECASE),
    re.compile(
        r"(?:semana\s+)?del\s+(\d{1,2})\s+(?:al|-)\s+(\d{1,2}\s+de\s+[a-záéíóú]+)\s+de\s+(\d{4})",
        re.IGNORECASE),
    re.compile(
        r"(?:semana\s+)?del\s+(\d{1,2})\s+(?:al|-)\s+(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})",
        re.IGNORECASE),
]

SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}

DEC_RE = re.compile(r"([\d\.\,]+)")

# -------------------------
# Utilidades
# -------------------------
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

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
    low = re.sub(r"\(.*?\)", "", low)
    low = re.sub(r"\s+", " ", low).strip()
    for k, v in LABEL_SYNONYMS.items():
        if k in low:
            return v
    if "fuel" in low and "oil" in low and "1" in low and "s" in low:
        return "Fuel Oil 1%S"
    if "fuel" in low and "oil" in low and "#6" in low:
        return "Fuel Oil #6"
    return None

# -------------------------
# Fuente (lista de PDFs)
# -------------------------
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

# -------------------------
# PDF: texto completo
# -------------------------
def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        pages = [p.extract_text() or "" for p in pdf.pages]
    return "\n".join(pages)

# -------------------------
# Buscar bloque "Precio(s) al público"
# -------------------------
def find_public_block(text: str) -> str | None:
    # Normaliza para hacer el matching robusto
    norm = strip_accents(text).lower()

    # Arranque del bloque (singular/plural, con/sin acento)
    start_terms = [
        "precio al publico",
        "precios al publico",
        "precios de venta al publico",
        "precio de venta al publico",
    ]
    start_idx = -1
    for term in start_terms:
        start_idx = norm.find(term)
        if start_idx != -1:
            break
    if start_idx == -1:
        return None

    # Corte del bloque (antes de Paridad / referencias / otra sección)
    end_terms = [
        "paridad",                # "precios de paridad", "paridad de importacion", etc.
        "precios de paridad",
        "referencia",             # por si usan "precio de referencia"
        "precio de referencia",
        "tasas de",               # a veces listan tasas abajo
        "nota:",
    ]
    end_idx = len(norm)
    for term in end_terms:
        idx = norm.find(term, start_idx + 1)
        if idx != -1:
            end_idx = min(end_idx, idx)

    # Regresa el bloque original (conformado al texto original, no normalizado)
    return text[start_idx:end_idx]

# -------------------------
# Parsear precios del público desde el bloque
# -------------------------
def parse_public_prices_from_block(block: str) -> list[dict]:
    if not block:
        return []

    items = []

    # Construimos un set de patrones por etiqueta para capturar "RD$ 290.10" o "290.10"
    label_variants = {
        "Gasolina Premium":   [r"gasolina\s+premium"],
        "Gasolina Regular":   [r"gasolina\s+regular"],
        "Gasoil Regular":     [r"gasoil\s+regular"],
        "Gasoil Óptimo":      [r"gasoil\s+optimo", r"gasoil\s+óptimo"],
        "Avtur":              [r"avtur"],
        "Kerosene":           [r"keros[eé]ne", r"kerosene"],
        "Fuel Oil #6":        [r"fuel\s*oil\s*#?6", r"fueloil\s*#?6"],
        "Fuel Oil 1%S":       [r"fuel\s*oil\s*1\s*%?\s*s", r"fueloil\s*1\s*%?\s*s"],
        "Gas Licuado de Petróleo": [r"gas\s+licuado\s+de\s+petr[oó]leo(?:\s*\(glp\))?", r"glp"],
        "Gas Natural":        [r"gas\s+natural"],
    }

    # número: RD$ opcional, espacios, decimales con coma o punto
    num_pat = r"(?:RD\$\s*)?([\d\.,]+)"
    flags = re.IGNORECASE | re.DOTALL

    # Para evitar capturar otra cifra en la misma línea, limitamos la búsqueda
    # a unos cuantos caracteres después de la etiqueta.
    window = 120  # caracteres después de la coincidencia del label

    block_compact = re.sub(r"[ \t]+", " ", block)

    for label, variants in label_variants.items():
        found = False
        for v in variants:
            # Buscar la etiqueta
            for m in re.finditer(v, block_compact, flags):
                start = m.end()
                snippet = block_compact[start:start + window]
                mnum = re.search(num_pat, snippet, flags)
                if mnum:
                    price = parse_num(mnum.group(1))
                    unit = "m3" if label.lower() == "gas natural" else "galon"
                    items.append({
                        "label": label,
                        "key": norm_key(label),
                        "price_dop": round(price, 2),
                        "unit": unit,
                        "change": None
                    })
                    found = True
                    break
            if found:
                break

    # dedup por key (por si el PDF repite)
    dedup, seen = [], set()
    for it in items:
        if it["key"] in seen:
            continue
        seen.add(it["key"])
        dedup.append(it)

    return dedup

# -------------------------
# Semana (rango de fechas)
# -------------------------
def _parse_spanish_date(fragment: str, default_year: int | None = None, default_month: int | None = None):
    frag = fragment.strip().lower()
    m = re.match(r"(\d{1,2})\s+de\s+([a-záéíóú]+)", frag, flags=re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month_name = strip_accents(m.group(2))
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

# -------------------------
# MAIN
# -------------------------
def main():
    pdf_url, link_text = get_latest_pdf_url_and_text()

    # Descarga bytes para extraer texto
    r = requests.get(pdf_url, timeout=60, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    pdf_bytes = r.content

    # Texto completo
    pdf_text = extract_text_from_pdf_bytes(pdf_bytes)

    # 1) Encuentra el bloque "Precio(s) al público"
    public_block = find_public_block(pdf_text)

    # 2) Parsear precios SOLO de ese bloque
    items = parse_public_prices_from_block(public_block) if public_block else []

    # 3) Si no encontró, NO publicar paridad (items vacío)
    if not items:
        items = []

    # 4) Semana (si se puede)
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
