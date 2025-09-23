# scraper/scraper.py
import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import pytesseract
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

# Aliases que buscamos en el texto OCR por línea (minúsculas)
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
FUEL_ORDER = ["Gasolina Premium","Gasolina Regular","Gasoil Regular","Gasoil Óptimo","Avtur","Kerosene","Fueloil #6","Fueloil 1%S","Gas Licuado de Petróleo (GLP)","Gas Natural"]

STOP_MARKERS = [
    "paridad de importación", "paridad de importacion",
    "estructura de precios", "precio paridad",
]

NUM_RE = re.compile(r"(?<!\d)(\d{2,4}[.,]\d{2})(?!\d)")

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

def pick_first_pdf(list_url: str):
    r = requests.get(list_url, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    links = [urljoin(list_url, a["href"]) for a in soup.select("a[href$='.pdf']")]
    # prioriza PDFs de 2025; si no, devuelve el primero
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

# ---------- OCR POSICIONAL (por palabras) ----------
def ocr_image_to_lines(img: Image.Image, lang="spa+eng"):
    """
    Devuelve una lista de líneas con posiciones y sus palabras:
    [{ 'text': '...', 'y': center_y, 'x_min':..., 'x_max':..., 'words': [(w, x1, y1, x2, y2)] }, ...]
    """
    data = pytesseract.image_to_data(img, lang=lang, config="--psm 6", output_type=pytesseract.Output.DICT)
    n = len(data["text"])
    lines = {}
    for i in range(n):
        text = (data["text"][i] or "").strip()
        conf = int(data["conf"][i]) if data["conf"][i].isdigit() else -1
        if conf < 0 or not text:
            continue
        page_num = data.get("page_num",[0])[i]
        block_num= data.get("block_num",[0])[i]
        par_num  = data.get("par_num",[0])[i]
        line_num = data.get("line_num",[0])[i]
        left = data["left"][i]; top = data["top"][i]
        w = data["width"][i]; h = data["height"][i]
        key = (page_num, block_num, par_num, line_num)
        rec = lines.get(key, {"words":[], "x_min":1e9, "x_max":-1, "y_vals":[]})
        rec["words"].append((text, left, top, left+w, top+h))
        rec["x_min"] = min(rec["x_min"], left)
        rec["x_max"] = max(rec["x_max"], left+w)
        rec["y_vals"].append(top + h/2)
        lines[key] = rec
    out = []
    for key, rec in sorted(lines.items(), key=lambda kv: (kv[0][0], kv[0][1], kv[0][2], kv[0][3])):
        words_sorted = sorted(rec["words"], key=lambda t: (t[1], t[2]))
        text = " ".join(w[0] for w in words_sorted)
        y_center = sum(rec["y_vals"])/len(rec["y_vals"])
        out.append({
            "key": key, "text": text, "text_l": text.lower(),
            "x_min": rec["x_min"], "x_max": rec["x_max"],
            "y": y_center, "words": words_sorted
        })
    return out

def ocr_pdf_to_lines(pdf_bytes: bytes, pages=(0,1), dpi=320, lang="spa+eng"):
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
            # Adjunta el índice de página (para segmentar por ancla)
            for rec in page_lines:
                rec["page_index"] = i
            lines.extend(page_lines)
    return lines

# Buscar región entre el encabezado de "precio oficial..." y los marcadores de fin
def slice_official_region(lines):
    start_idx = None
    end_idx = None
    for idx, rec in enumerate(lines):
        if "precio oficial a pagar por el p" in rec["text_l"] or "precio oficial a pagar" in rec["text_l"]:
            start_idx = idx
            break
    if start_idx is None:
        return lines  # si no se encontró, devolvemos todo (fallback)
    # Buscar primer STOP marcando fin
    for idx in range(start_idx+1, len(lines)):
        tl = lines[idx]["text_l"]
        if any(m in tl for m in STOP_MARKERS):
            end_idx = idx
            break
    return lines[start_idx: end_idx] if end_idx else lines[start_idx:]

def parse_price_from_line_words(line_rec):
    """Extrae el número más a la derecha en esa línea."""
    nums = []
    for w,left,top,right,bottom in line_rec["words"]:
        m = NUM_RE.fullmatch(w) or re.search(NUM_RE, w)
        if m:
            val = m.group(1).replace(",", ".")
            try:
                price = float(val)
                nums.append((price, right))  # usamos 'right' para elegir el más a la derecha
            except:
                pass
    if not nums:
        return None
    # número más a la derecha
    nums.sort(key=lambda t: t[1])
    return nums[-1][0]

def nearest_price_same_or_next_line(lines, idx):
    """Busca precio en la misma línea; si no, en la siguiente inmediata (misma página/bloque/par)."""
    # misma línea
    p = parse_price_from_line_words(lines[idx])
    if p is not None:
        return p
    # siguiente línea, si es el mismo bloque/parrafo (clave coincide salvo line_num)
    page, block, par, line = lines[idx]["key"]
    for j in range(idx+1, min(idx+4, len(lines))):
        p2, b2, pa2, l2 = lines[j]["key"]
        if p2 == page and b2 == block and pa2 == par and l2 in (line+1, line+2):
            # Evita capturar variaciones tipo "sube/baja"
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
        # encuentra la primera línea de la región que contenga el alias
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

        # saneo básico: precios al público por galón suelen estar entre 40 y 400
        # (Gas Natural m³ ~ 30–60; igual está dentro)
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

# (Opcional) detectar semana “del X al Y de <mes> de YYYY”
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

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

    pdf_url, pdf_bytes = get_latest_pdf()

    # OCR posicional de las 1–2 primeras páginas (donde viene la tabla oficial)
    lines = ocr_pdf_to_lines(pdf_bytes, pages=(0,1), dpi=330, lang="spa+eng")

    items = build_items_from_lines(lines)
    # safety-net: si no encontró nada, intenta sobre TODAS las líneas (sin recorte de región)
    if not items:
        items = build_items_from_lines(lines=lines)

    s, e = parse_week_from_lines(lines)

    payload = {
        "source": pdf_url,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {"start_date": s, "end_date": e},
        "currency": "DOP",
        "items": items
    }

    with open(os.path.join(OUT_DIR, "latest.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    stamp = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    with open(os.path.join(HIST_DIR, f"{stamp}.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
