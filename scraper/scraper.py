# scraper/scraper.py
import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import pytesseract
from pytesseract import Output
from PIL import Image

# URLs del MICM
MICM_2025_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/avisos-semanales-de-precios-de-combustibles-2025/"
MICM_FALLBACK_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

# Canonicalización de llaves
FUEL_KEYS = {
    "gasolina premium": "gasolina_premium",
    "gasolina regular": "gasolina_regular",
    "gasoil regular":   "gasoil_regular",
    "gasoil óptimo":    "gasoil_optimo",
    "gasoil optimo":    "gasoil_optimo",
    "avtur":            "avtur",
    "kerosene":         "kerosene",
    "fueloil #6":       "fueloil_6",
    "fuel oil #6":      "fueloil_6",
    "fuel oil":         "fueloil_6",
    "fueloil":          "fueloil_6",
    "fueloil 1%s":      "fueloil_1s",
    "fuel oil 1%s":     "fueloil_1s",
    "gas licuado de petróleo (glp)": "glp",
    "gas licuado de petroleo (glp)": "glp",
    "glp":              "glp",
    "gas natural":      "gas_natural",
    "cilindros de 100 libras": "cilindro_100lb",
    "cilindros de 50 libras":  "cilindro_50lb",
    "cilindros de 25 libras":  "cilindro_25lb",
    "cilindros de 15 libras":  "cilindro_15lb",
}

# Aliases que buscamos por línea (minúsculas)
LABEL_ALIASES = {
    "Gasolina Premium":  ["gasolina premium", "gas. premium", "gasolina prem"],
    "Gasolina Regular":  ["gasolina regular", "gas. regular", "gasolina reg"],
    "Gasoil Regular":    ["gasoil regular"],
    "Gasoil Óptimo":     ["gasoil óptimo", "gasoil optimo", "diesel optimo", "diésel óptimo", "diesel óptimo"],
    "Avtur":             ["avtur"],
    "Kerosene":          ["kerosene", "queroseno", "kerosén"],
    # MICM publishes the regular FO row as just "Fuel Oil" (no #6 suffix).
    # Keep both — the longer aliases match first so we don't accidentally
    # capture FO 1% Azufre into this bucket.
    "Fueloil #6":        ["fueloil #6", "fuel oil #6", "fuel oil # 6", "fuel-oil #6", "fuel oil"],
    "Fueloil 1%S":       ["fueloil 1%s", "fuel oil 1%s", "fuel oil 1 %s", "fueloil 1 %s",
                          "fuel oil 1% azufre", "fuel oil 1 % azufre", "fueloil 1% azufre"],
    "Gas Licuado de Petróleo (GLP)": ["gas licuado de petróleo (glp)", "gas licuado de petroleo (glp)", "glp"],
    "Gas Natural":       ["gas natural"],
    # GLP retail cylinders ("envasadoras") — published as a separate block in
    # the same PDF. Households buy GLP this way, so the prices are useful.
    "Cilindro 100 lb":   ["cilindros de 100 libras", "cilindro de 100 libras"],
    "Cilindro 50 lb":    ["cilindros de 50 libras",  "cilindro de 50 libras"],
    "Cilindro 25 lb":    ["cilindros de 25 libras",  "cilindro de 25 libras"],
    "Cilindro 15 lb":    ["cilindros de 15 libras",  "cilindro de 15 libras"],
}
# Orden de salida
FUEL_ORDER = [
    "Gasolina Premium","Gasolina Regular","Gasoil Regular","Gasoil Óptimo",
    "Avtur","Kerosene","Fueloil #6","Fueloil 1%S",
    "Gas Licuado de Petróleo (GLP)","Gas Natural",
    "Cilindro 100 lb","Cilindro 50 lb","Cilindro 25 lb","Cilindro 15 lb",
]

STOP_MARKERS = [
    "paridad de importación", "paridad de importacion",
    "estructura de precios", "precio paridad",
]

# Number with 2 decimals. Accepts thousand-separated form ("3,429.95") for
# cylinder prices AND the bare form for fuel prices ("307.50"). Matches must
# stand on a non-digit boundary.
NUM_RE = re.compile(
    r"(?<!\d)(\d{1,3}(?:,\d{3})+\.\d{2}|\d{2,5}[.,]\d{2})(?!\d)"
)

# Plausible price range per canonical fuel label. Cylinders live in a much
# higher band than liquid fuels (per-gallon). Liquid fuels default to
# 50–500 DOP/gal which excludes the variation-column noise (typically
# under 30) that was getting picked up for Avtur/Kerosene/Premium.
PRICE_RANGE_DEFAULT = (50.0, 500.0)
PRICE_RANGES = {
    "Cilindro 100 lb": (2000.0, 6000.0),
    "Cilindro 50 lb":  (1000.0, 3000.0),
    "Cilindro 25 lb":  (400.0,  1500.0),
    "Cilindro 15 lb":  (250.0,  900.0),
}

# Unit override per canonical fuel label.
UNIT_OVERRIDES = {
    "Gas Natural":     "m3",
    "Cilindro 100 lb": "cilindro",
    "Cilindro 50 lb":  "cilindro",
    "Cilindro 25 lb":  "cilindro",
    "Cilindro 15 lb":  "cilindro",
}

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

def pick_first_pdf(list_url: str):
    r = requests.get(list_url, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    links = [urljoin(list_url, a["href"]) for a in soup.select("a[href$='.pdf']")]
    for u in links:
        if "2025" in u:
            return u
    return links[0] if links else None

def get_latest_pdf():
    url = pick_first_pdf(MICM_2025_URL) or pick_first_pdf(MICM_FALLBACK_URL)
    if not url:
        raise RuntimeError("No se encontraron PDFs del MICM.")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return url, resp.content

# ---------- OCR POSICIONAL ----------
def _to_int(x, default=0):
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return default

def _to_float(x, default=-1.0):
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).replace(",", "."))
        except Exception:
            return default

def ocr_image_to_lines(img: Image.Image, lang="spa+eng"):
    """
    Devuelve líneas con posiciones y sus palabras:
    [{ 'text': '...', 'y': center_y, 'x_min':..., 'x_max':..., 'words': [(w, x1, y1, x2, y2)] }, ...]
    """
    data = pytesseract.image_to_data(img, lang=lang, config="--psm 6", output_type=Output.DICT)
    n = len(data.get("text", []))
    lines = {}
    for i in range(n):
        text_raw = data["text"][i]
        text = ("" if text_raw is None else str(text_raw)).strip()

        conf_raw = data.get("conf", ["-1"]*n)[i]
        conf = _to_float(conf_raw, default=-1.0)

        if not text or conf < 0:  # si conf es -1 (ruido), lo saltamos
            continue

        left  = _to_int(data.get("left",  [0]*n)[i], 0)
        top   = _to_int(data.get("top",   [0]*n)[i], 0)
        width = _to_int(data.get("width", [0]*n)[i], 0)
        height= _to_int(data.get("height",[0]*n)[i], 0)

        page_num = _to_int(data.get("page_num",[0]*n)[i], 0)
        block_num= _to_int(data.get("block_num",[0]*n)[i], 0)
        par_num  = _to_int(data.get("par_num",  [0]*n)[i], 0)
        line_num = _to_int(data.get("line_num", [0]*n)[i], 0)

        key = (page_num, block_num, par_num, line_num)
        rec = lines.get(key, {"words":[], "x_min":10**9, "x_max":-1, "y_vals":[]})
        rec["words"].append((text, left, top, left+width, top+height))
        rec["x_min"] = min(rec["x_min"], left)
        rec["x_max"] = max(rec["x_max"], left+width)
        rec["y_vals"].append(top + height/2)
        lines[key] = rec

    out = []
    for key, rec in sorted(lines.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], kv[0][3])):
        words_sorted = sorted(rec["words"], key=lambda t: (t[1], t[2]))
        text = " ".join(w[0] for w in words_sorted)
        y_center = sum(rec["y_vals"])/len(rec["y_vals"]) if rec["y_vals"] else 0
        out.append({
            "key": key, "text": text, "text_l": text.lower(),
            "x_min": rec["x_min"], "x_max": rec["x_max"],
            "y": y_center, "words": words_sorted
        })
    return out

def ocr_pdf_to_lines(pdf_bytes: bytes, pages=(0,1), dpi=330, lang="spa+eng"):
    lines = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        max_page = min(doc.page_count, (pages[1]+1) if isinstance(pages, tuple) else doc.page_count)
        for i in range(pages[0], max_page):
            page = doc.load_page(i)
            pix = page.get_pixmap(dpi=dpi)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            try:
                page_lines = ocr_image_to_lines(img, lang=lang)
            except Exception:
                page_lines = ocr_image_to_lines(img, lang="eng")
            for rec in page_lines:
                rec["page_index"] = i
            lines.extend(page_lines)
    return lines

# Región entre “PRECIO OFICIAL…” y el siguiente bloque (paridad/estructura)
def slice_official_region(lines):
    start_idx = None
    end_idx = None
    for idx, rec in enumerate(lines):
        if "precio oficial a pagar por el p" in rec["text_l"] or "precio oficial a pagar" in rec["text_l"]:
            start_idx = idx
            break
    if start_idx is None:
        return lines  # fallback: todo
    for idx in range(start_idx+1, len(lines)):
        tl = lines[idx]["text_l"]
        if any(m in tl for m in STOP_MARKERS):
            end_idx = idx
            break
    return lines[start_idx: end_idx] if end_idx else lines[start_idx:]

def _parse_number(token: str) -> float | None:
    """Parse a single OCR token to float, handling 'RD$1,234.56' style."""
    val = token.replace(",", "")  # strip thousand separators
    # The dot in '1234.56' is the decimal point, so just float() works.
    try:
        return float(val)
    except ValueError:
        # OCR sometimes substitutes "1234,56" (comma decimal). Retry.
        try:
            return float(token.replace(",", "."))
        except ValueError:
            return None


def parse_price_from_line_words(line_rec):
    """
    Pick the most likely "consumer price" from an OCR'd table row.

    The MICM aviso has this column order per fuel row:

        TIPO | PARIDAD | LEY 112-00 | LEY 495-06 | DIST | DETAL | TRANSP
             | PRECIO_OFICIAL | AJUSTE | PRECIO_PUBLICO | VARIACION

    The previous picker took the rightmost numeric token. That breaks for
    Avtur / Kerosene / Premium because the VARIACION column (rightmost
    column for the rows in this aviso) holds a small positive or negative
    number like (4.80), (23.15), 4.00 — wrapped in parens for decreases.

    Heuristic, in order:
      1. Reject any number whose OCR token includes '(' or ')'
         (variation column always appears parens-wrapped, even when the
         delta is positive in some weeks).
      2. Among remaining numbers, prefer the rightmost one in the
         typical fuel price range (≥ 100 DOP). This corresponds to the
         PRECIO_PUBLICO column when OCR groups it onto the row, or
         PRECIO_OFICIAL otherwise — both are sensible answers.
      3. If nothing ≥ 100 survives, fall back to the rightmost ≥ 50
         (covers GLP at ~137 and any future low-priced fuel).
      4. Last resort: rightmost any number (legacy behavior).
    """
    candidates = []
    for w, left, top, right, bottom in line_rec["words"]:
        in_parens = "(" in w or ")" in w
        for m in NUM_RE.finditer(w):
            tok = m.group(1)
            price = _parse_number(tok)
            if price is None:
                continue
            candidates.append((price, right, in_parens))

    if not candidates:
        return None

    free = [c for c in candidates if not c[2]]

    # Step 2 — rightmost in typical fuel band
    typical = [c for c in free if c[0] >= 100.0]
    if typical:
        typical.sort(key=lambda t: t[1])
        return typical[-1][0]

    # Step 3 — rightmost in extended sane band
    sane = [c for c in free if c[0] >= 50.0]
    if sane:
        sane.sort(key=lambda t: t[1])
        return sane[-1][0]

    # Step 4 — fallback: rightmost anything (paren or not)
    candidates.sort(key=lambda t: t[1])
    return candidates[-1][0]

def nearest_price_same_or_next_line(lines, idx):
    p = parse_price_from_line_words(lines[idx])
    if p is not None:
        return p
    page, block, par, line = lines[idx]["key"]
    for j in range(idx+1, min(idx+4, len(lines))):
        p2, b2, pa2, l2 = lines[j]["key"]
        if p2 == page and b2 == block and pa2 == par and l2 in (line+1, line+2):
            txt = lines[j]["text_l"]
            if any(w in txt for w in ["sube", "baja", "mantiene", "variación", "variacion"]):
                continue
            price = parse_price_from_line_words(lines[j])
            if price is not None:
                return price
        else:
            break
    return None

def norm_key(label: str) -> str:
    k = label.lower().strip()
    for needle, norm in FUEL_KEYS.items():
        if needle in k:
            return norm
    return re.sub(r"[^a-z0-9]+", "_", k)

def build_items_from_lines(lines):
    region = slice_official_region(lines)
    items = []
    seen_keys = set()

    # For cylinder rows the MICM publishes them BELOW "Precio de Venta del
    # GLP al Público en las Envasadoras", which falls outside the
    # `slice_official_region` window (we cut at the stop markers above).
    # Search the full `lines` for cylinder labels so we catch them too.
    cylinder_labels = {
        "Cilindro 100 lb", "Cilindro 50 lb", "Cilindro 25 lb", "Cilindro 15 lb",
    }

    for canonical_label in FUEL_ORDER:
        aliases = LABEL_ALIASES[canonical_label]
        haystack = lines if canonical_label in cylinder_labels else region

        cand_idx = None
        for i, rec in enumerate(haystack):
            tl = rec["text_l"]
            if any(alias in tl for alias in aliases):
                cand_idx = i
                break
        if cand_idx is None:
            continue

        price = nearest_price_same_or_next_line(haystack, cand_idx)
        if price is None:
            continue

        lo, hi = PRICE_RANGES.get(canonical_label, PRICE_RANGE_DEFAULT)
        if not (lo <= price <= hi):
            continue

        unit = UNIT_OVERRIDES.get(canonical_label, "galon")
        key = norm_key(canonical_label)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        items.append({
            "label": canonical_label,
            "key": key,
            "price_dop": round(price, 2),
            "unit": unit,
            "change": None
        })
    return items

# --- Detección de la semana de vigencia ---
# La MICM publica el aviso semanal con el rango "del DD al DD de MES de AAAA".
# Hay varias variaciones (a veces con "del DD de MES al DD de MES", a veces
# saltos de meses cuando cubre fin-de-mes, a veces OCR pierde tildes/acentos).
# Probamos varios patrones en orden y devolvemos el primer match válido.
SPANISH_MONTHS = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}

SPANISH_MONTHS_SHORT_ES = ["ene","feb","mar","abr","may","jun","jul","ago","sep","oct","nov","dic"]


def _normalize_es(s: str) -> str:
    """Strip Spanish accents/tildes so regex is more permissive vs OCR errors."""
    return (s or "").lower() \
        .replace("á","a").replace("é","e").replace("í","i") \
        .replace("ó","o").replace("ú","u").replace("ñ","n")


def parse_week_from_lines(lines):
    """
    Returns (start_iso, end_iso) — both 'YYYY-MM-DD' — or (None, None).
    Tries multiple shapes that the MICM has used over time. Patterns that
    match the regex but produce a date pair that doesn't validate (negative
    span, out-of-range, etc.) fall through to the next pattern instead of
    short-circuiting.
    """
    # Concatenate the first chunk of OCR text (header normally).
    text_raw = "  ".join(rec["text_l"] for rec in lines[:120])
    text = _normalize_es(text_raw)

    # --- Pattern B (cross-month, most explicit) FIRST so we don't get fooled
    # by Pattern A swallowing a multi-month range like
    # "del 30 de mayo al 5 de junio".
    m = re.search(
        r"del\s+(\d{1,2})\s+de\s+([a-z]+).{0,40}?al\s+(\d{1,2})\s+de\s+([a-z]+).{0,30}?de\s+(\d{4})",
        text,
    )
    if m:
        d1, mon1, d2, mon2, y = m.groups()
        month1 = SPANISH_MONTHS.get(mon1)
        month2 = SPANISH_MONTHS.get(mon2)
        if month1 and month2:
            y = int(y); d1 = int(d1); d2 = int(d2)
            # If start month is December and end month is January → end year is +1.
            end_year = y + 1 if (month1 == 12 and month2 == 1) else y
            start_year = y - 1 if (month1 == 12 and month2 == 1 and end_year == y + 1) else y
            # Reset: most documents quote only the END year for dec→jan ranges,
            # but the start was in the previous calendar year.
            if month1 == 12 and month2 == 1:
                start_year = y; end_year = y + 1
            s, e = _safe_pair(start_year, month1, d1, end_year, month2, d2)
            if s and e:
                return s, e

    # --- Pattern A (same month at the end): "del 30 al 5 de junio de 2026"
    # Note that when d1 > d2 with a single month tail, the intended start is
    # actually the *previous* month (e.g. "del 30 al 5 de junio" = May 30 → Jun 5).
    m = re.search(
        r"del\s+(\d{1,2})\b.{0,30}?al\s+(\d{1,2})\b.{0,30}?de\s+([a-z]+).{0,30}?de\s+(\d{4})",
        text,
    )
    if m:
        d1, d2, mon, y = m.groups()
        month = SPANISH_MONTHS.get(mon)
        if month:
            y = int(y); d1 = int(d1); d2 = int(d2)
            if d1 <= d2:
                # Same month.
                s, e = _safe_pair(y, month, d1, y, month, d2)
            else:
                # d1 > d2 → d1 belongs to the previous month.
                prev_month = month - 1 or 12
                prev_year = y - 1 if month == 1 else y
                s, e = _safe_pair(prev_year, prev_month, d1, y, month, d2)
            if s and e:
                return s, e

    # --- Pattern C: vigencia/vigente prefix, same-month tail.
    m = re.search(
        r"(?:vigencia|vigente)[^0-9]{0,40}(\d{1,2})\b.{0,30}?al\s+(\d{1,2})\b.{0,30}?de\s+([a-z]+).{0,30}?(\d{4})",
        text,
    )
    if m:
        d1, d2, mon, y = m.groups()
        month = SPANISH_MONTHS.get(mon)
        if month:
            y = int(y); d1 = int(d1); d2 = int(d2)
            if d1 <= d2:
                s, e = _safe_pair(y, month, d1, y, month, d2)
            else:
                prev_month = month - 1 or 12
                prev_year = y - 1 if month == 1 else y
                s, e = _safe_pair(prev_year, prev_month, d1, y, month, d2)
            if s and e:
                return s, e

    return None, None


def _safe_pair(y1, m1, d1, y2, m2, d2):
    """Validate two YYYY-MM-DD dates make sense; return ISO strings or (None, None)."""
    try:
        start = datetime.date(y1, m1, d1)
        end = datetime.date(y2, m2, d2)
    except ValueError:
        return None, None
    if (end - start).days < 0 or (end - start).days > 14:
        # MICM weeks are 7 days; allow up to 14 to be safe but reject obvious junk.
        return None, None
    return start.isoformat(), end.isoformat()


# Spanish month abbreviations as they appear in MICM PDF filenames.
SPANISH_MONTH_ABBR_3 = {
    "ENE":1, "FEB":2, "MAR":3, "ABR":4, "MAY":5, "JUN":6,
    "JUL":7, "AGO":8, "SEP":9, "SET":9, "OCT":10, "NOV":11, "DIC":12,
}


def parse_week_from_pdf_url(pdf_url: str):
    """
    M9: filename-fallback for week detection.
    The MICM publishes PDFs whose URL embeds the week of validity, e.g.
        AVISO-PRE.-SEM.CORTE-30-MAY-05-JUN-DE-2026-.pdf
    The OCR sometimes misses the textual "del DD al DD de MES de YYYY" line
    (slice region cuts it, or low confidence). The filename is far more
    deterministic — we use it as a second-pass extractor.

    Returns (start_iso, end_iso) or (None, None).
    """
    if not pdf_url:
        return None, None
    fn = pdf_url.rsplit("/", 1)[-1].rsplit(".", 1)[0].upper()

    # Shape A: DD-MON-DD-MON-(DE-)?YYYY  ← most common 2025/2026
    m = re.search(
        r"(?<!\d)(\d{1,2})[-_.]+([A-Z]{3})[-_.]+(\d{1,2})[-_.]+([A-Z]{3})(?:[-_.]+DE)?[-_.]+(\d{4})",
        fn,
    )
    if m:
        d1, mo1, d2, mo2, y = m.groups()
        m1 = SPANISH_MONTH_ABBR_3.get(mo1)
        m2 = SPANISH_MONTH_ABBR_3.get(mo2)
        if m1 and m2:
            y = int(y); d1 = int(d1); d2 = int(d2)
            # Cross-year handling: e.g. "30-DIC-05-ENE-DE-2026" probably means
            # the start was Dec 30 of the previous year. MICM convention is
            # usually to embed the END year, so start moves to (y-1).
            if m1 == 12 and m2 == 1:
                return _safe_pair(y - 1, m1, d1, y, m2, d2)
            return _safe_pair(y, m1, d1, y, m2, d2)

    # Shape B: DD-MM-YYYY-AL-DD-MM-YYYY (purely numeric range)
    m = re.search(
        r"(\d{1,2})[-_.](\d{1,2})[-_.](\d{4}).{0,8}?AL.{0,8}?(\d{1,2})[-_.](\d{1,2})[-_.](\d{4})",
        fn,
    )
    if m:
        d1, mo1, y1, d2, mo2, y2 = m.groups()
        return _safe_pair(int(y1), int(mo1), int(d1), int(y2), int(mo2), int(d2))

    # Shape C: long form with month names: "09-DE-AGOSTO-AL-15-DE-AGOSTO-2025"
    LONG_MONTHS = {
        "ENERO":1, "FEBRERO":2, "MARZO":3, "ABRIL":4, "MAYO":5, "JUNIO":6,
        "JULIO":7, "AGOSTO":8, "SEPTIEMBRE":9, "SETIEMBRE":9,
        "OCTUBRE":10, "NOVIEMBRE":11, "DICIEMBRE":12,
    }
    m = re.search(
        r"(\d{1,2}).{0,5}?DE.{0,5}?([A-Z]+).{0,8}?AL.{0,5}?(\d{1,2}).{0,5}?DE.{0,5}?([A-Z]+).{0,8}?(\d{4})",
        fn,
    )
    if m:
        d1, mo1, d2, mo2, y = m.groups()
        m1 = LONG_MONTHS.get(mo1)
        m2 = LONG_MONTHS.get(mo2)
        if m1 and m2:
            y = int(y); d1 = int(d1); d2 = int(d2)
            start_y = y - 1 if (m1 == 12 and m2 == 1) else y
            return _safe_pair(start_y, m1, d1, y, m2, d2)

    return None, None


def build_week_label(start_iso, end_iso):
    """
    Human-friendly Spanish label for the week, e.g.:
      "Vigente: 30 may – 5 jun 2026"
      "Vigente: 1 – 7 jun 2026"        (same month)
      "Vigente: 30 dic 2025 – 5 ene 2026"  (cross-year)
    Returns None if dates are missing or unparseable.
    """
    if not start_iso or not end_iso:
        return None
    try:
        s = datetime.date.fromisoformat(start_iso)
        e = datetime.date.fromisoformat(end_iso)
    except Exception:
        return None
    sm = SPANISH_MONTHS_SHORT_ES[s.month - 1]
    em = SPANISH_MONTHS_SHORT_ES[e.month - 1]
    if s.year != e.year:
        return f"Vigente: {s.day} {sm} {s.year} – {e.day} {em} {e.year}"
    if s.month != e.month:
        return f"Vigente: {s.day} {sm} – {e.day} {em} {e.year}"
    return f"Vigente: {s.day} – {e.day} {sm} {e.year}"

# Minimum number of fuel items we expect from a healthy run.
# The MICM publication usually lists 8–10 products. If the OCR returns less
# than this, we assume the parse degraded (bad PDF render, OCR confidence
# dropped, etc.) and we DO NOT overwrite latest.json — we keep the previous
# good snapshot live until the next run succeeds. This is M4 from Sprint 1.
MIN_ITEMS_THRESHOLD = 5


def _utc_now_iso() -> str:
    """UTC timestamp string; uses timezone-aware API (utcnow() is deprecated in 3.12+)."""
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_previous_latest():
    """Load the previously published latest.json, if any. Returns None on miss."""
    path = os.path.join(OUT_DIR, "latest.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _compute_change(new_items, prev_payload):
    """
    M2 from Sprint 1.
    For each item in `new_items`, attach a `change` dict comparing against the
    previous week's price (looked up by `key`):
        - {"type": "up", "amount_dop": +N.NN}
        - {"type": "down", "amount_dop": -N.NN}
        - {"type": "same", "amount_dop": 0.0}
    Items the previous payload didn't have keep `change: null`.
    Mutates `new_items` in place and returns it.
    """
    if not prev_payload:
        return new_items
    prev_by_key = {it.get("key"): it for it in (prev_payload.get("items") or [])}
    for it in new_items:
        prev = prev_by_key.get(it.get("key"))
        if not prev or prev.get("price_dop") is None:
            continue
        try:
            delta = round(float(it["price_dop"]) - float(prev["price_dop"]), 2)
        except (TypeError, ValueError):
            continue
        if delta > 0:
            it["change"] = {"type": "up", "amount_dop": delta}
        elif delta < 0:
            it["change"] = {"type": "down", "amount_dop": delta}
        else:
            it["change"] = {"type": "same", "amount_dop": 0.0}
    return new_items


def _items_payload_signature(items):
    """Stable hash-friendly representation of items (ignoring `change` field).
    Used to detect "real" change vs no-op runs that publish same prices."""
    minimal = sorted(
        [(it.get("key"), it.get("price_dop"), it.get("unit")) for it in items]
    )
    return json.dumps(minimal, ensure_ascii=False, sort_keys=True)


def main():
    ensure_dirs()
    pdf_url, pdf_bytes = get_latest_pdf()

    # OCR posicional de las 1–2 primeras páginas
    lines = ocr_pdf_to_lines(pdf_bytes, pages=(0,1), dpi=330, lang="spa+eng")

    items = build_items_from_lines(lines)
    if not items:
        # safety-net: intenta sobre TODAS las líneas
        items = build_items_from_lines(lines=lines)

    # ---- M4: gate publish on a sanity-check item count ----
    if len(items) < MIN_ITEMS_THRESHOLD:
        prev = _load_previous_latest()
        if prev:
            # Keep the last known good snapshot live; just log and exit cleanly.
            print(
                f"⚠️  OCR returned only {len(items)} items (< {MIN_ITEMS_THRESHOLD} threshold). "
                f"Keeping previous latest.json (published {prev.get('updated_at_utc')})."
            )
            return
        print(
            f"⚠️  OCR returned only {len(items)} items and no previous snapshot found. "
            f"Will write the partial payload anyway as a cold-start."
        )

    s, e = parse_week_from_lines(lines)
    # M9: filename fallback. The OCR sometimes misses the date line in the
    # header (slice region or low confidence). The PDF URL itself embeds the
    # week of validity and is far more deterministic.
    if not (s and e):
        fs, fe = parse_week_from_pdf_url(pdf_url)
        if fs and fe:
            print(f"ℹ️  week parsed from PDF filename fallback: {fs} → {fe}")
            s, e = fs, fe
        else:
            print(f"⚠️  could not detect week from OCR nor from filename ({pdf_url})")
    week_label = build_week_label(s, e)   # M6: human-friendly "Vigente: 30 may – 5 jun 2026"

    # ---- M2: compute per-item delta vs previous week ----
    prev_payload = _load_previous_latest()
    items = _compute_change(items, prev_payload)

    payload = {
        "source": pdf_url,
        "updated_at_utc": _utc_now_iso(),
        "week": {
            "start_date": s,
            "end_date": e,
            "label": week_label,
        },
        # Mirror the label at the top level too: the Swift client (FuelFeed)
        # already declares `week_label` as an optional decode target.
        "week_label": week_label,
        "currency": "DOP",
        "items": items
    }

    # ---- M3: write only if the canonical content actually changed ----
    # Compare only the start/end dates of the week (label is derived from
    # them — including it would force a false rewrite on label-format changes).
    if prev_payload:
        prev_week = prev_payload.get("week") or {}
        same_items = _items_payload_signature(items) == _items_payload_signature(prev_payload.get("items") or [])
        same_window = (prev_week.get("start_date") == s and prev_week.get("end_date") == e)
        if same_items and same_window:
            print("✅ No content changes vs previous publish — skipping write (workflow will skip commit too).")
            return

    with open(os.path.join(OUT_DIR, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    with open(os.path.join(HIST_DIR, f"{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
