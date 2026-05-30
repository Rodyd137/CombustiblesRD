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
    "fueloil 1%s":      "fueloil_1s",
    "fuel oil 1%s":     "fueloil_1s",
    "gas licuado de petróleo (glp)": "glp",
    "gas licuado de petroleo (glp)": "glp",
    "glp":              "glp",
    "gas natural":      "gas_natural",
}

# Aliases que buscamos por línea (minúsculas)
LABEL_ALIASES = {
    "Gasolina Premium":  ["gasolina premium", "gas. premium", "gasolina prem"],
    "Gasolina Regular":  ["gasolina regular", "gas. regular", "gasolina reg"],
    "Gasoil Regular":    ["gasoil regular"],
    "Gasoil Óptimo":     ["gasoil óptimo", "gasoil optimo", "diesel optimo", "diésel óptimo", "diesel óptimo"],
    "Avtur":             ["avtur"],
    "Kerosene":          ["kerosene", "queroseno", "kerosén"],
    "Fueloil #6":        ["fueloil #6", "fuel oil #6", "fuel oil # 6", "fuel-oil #6"],
    "Fueloil 1%S":       ["fueloil 1%s", "fuel oil 1%s", "fuel oil 1 %s", "fueloil 1 %s"],
    "Gas Licuado de Petróleo (GLP)": ["gas licuado de petróleo (glp)", "gas licuado de petroleo (glp)", "glp"],
    "Gas Natural":       ["gas natural"],
}
# Orden de salida
FUEL_ORDER = [
    "Gasolina Premium","Gasolina Regular","Gasoil Regular","Gasoil Óptimo",
    "Avtur","Kerosene","Fueloil #6","Fueloil 1%S","Gas Licuado de Petróleo (GLP)","Gas Natural"
]

STOP_MARKERS = [
    "paridad de importación", "paridad de importacion",
    "estructura de precios", "precio paridad",
]

# n con 2–4 dígitos enteros y dos decimales (ej. 290.10, 154,90, 43.97)
NUM_RE = re.compile(r"(?<!\d)(\d{2,4}[.,]\d{2})(?!\d)")

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

def parse_price_from_line_words(line_rec):
    """Número con dos decimales más a la derecha en esa línea."""
    nums = []
    for w,left,top,right,bottom in line_rec["words"]:
        m = NUM_RE.fullmatch(w) or re.search(NUM_RE, w)
        if m:
            val = m.group(1).replace(",", ".")
            try:
                price = float(val)
                nums.append((price, right))
            except:
                pass
    if not nums:
        return None
    nums.sort(key=lambda t: t[1])
    return nums[-1][0]

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

    for canonical_label in FUEL_ORDER:
        aliases = LABEL_ALIASES[canonical_label]
        cand_idx = None
        for i, rec in enumerate(region):
            tl = rec["text_l"]
            if any(alias in tl for alias in aliases):
                cand_idx = i
                break
        if cand_idx is None:
            continue

        price = nearest_price_same_or_next_line(region, cand_idx)
        if price is None:
            continue

        # filtro rango razonable
        if not (20.0 <= price <= 500.0):
            continue

        unit = "m3" if canonical_label.lower().startswith("gas natural") else "galon"
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

# (Opcional) semana “del X al Y de <mes> de YYYY”
SPANISH_MONTHS = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}
def parse_week_from_lines(lines):
    text = "  ".join(rec["text_l"] for rec in lines[:80])
    m = re.search(r"del\s+(\d{1,2}).{0,20}?al\s+(\d{1,2}).{0,20}?de\s+([a-záéíóúñ]+).{0,20}?de\s+(\d{4})", text, re.IGNORECASE)
    if not m:
        return None, None
    d1, d2, mon, y = m.groups()
    mon = (mon or "").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    month = SPANISH_MONTHS.get(mon, None)
    if not month:
        return None, None
    y = int(y); d1 = int(d1); d2 = int(d2)
    return (f"{y:04d}-{month:02d}-{d1:02d}", f"{y:04d}-{month:02d}-{d2:02d}")

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

    # ---- M2: compute per-item delta vs previous week ----
    prev_payload = _load_previous_latest()
    items = _compute_change(items, prev_payload)

    payload = {
        "source": pdf_url,
        "updated_at_utc": _utc_now_iso(),
        "week": {"start_date": s, "end_date": e},
        "currency": "DOP",
        "items": items
    }

    # ---- M3: write only if the canonical content actually changed ----
    if prev_payload:
        if _items_payload_signature(items) == _items_payload_signature(prev_payload.get("items") or []) \
           and (prev_payload.get("week") or {}) == payload["week"]:
            print("✅ No content changes vs previous publish — skipping write (workflow will skip commit too).")
            return

    with open(os.path.join(OUT_DIR, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    with open(os.path.join(HIST_DIR, f"{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
