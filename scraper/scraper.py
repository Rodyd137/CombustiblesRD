# scraper/scraper.py
import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import pytesseract
from PIL import Image

MICM_2025_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/avisos-semanales-de-precios-de-combustibles-2025/"
MICM_FALLBACK_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

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
    "fuel oil #6": "fueloil_6",
    "fueloil #6": "fueloil_6",
    "fuel oil 1%s": "fueloil_1s",
    "fueloil 1%s": "fueloil_1s",
    "fueloil 1% s": "fueloil_1s",
    "glp": "glp",
    "gas licuado de petróleo": "glp",
    "gas licuado de petroleo": "glp",
    "gas natural": "gas_natural",
}

FUEL_ORDER = [
    "gasolina premium", "gasolina regular",
    "gasoil regular", "gasoil óptimo", "gasoil optimo",
    "avtur", "kerosene", "fuel oil #6", "fueloil #6",
    "fuel oil 1%s", "fueloil 1%s", "glp", "gas licuado de petróleo",
    "gas licuado de petroleo", "gas natural"
]

STOP_MARKERS = [
    "paridad de importación", "paridad de importacion",
    "estructura de precios", "precio paridad",
]

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

def pick_first_pdf(list_url: str):
    r = requests.get(list_url, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    links = [urljoin(list_url, a["href"]) for a in soup.select("a[href$='.pdf']")]
    # prioriza PDFs 2025 y con "precios" en el nombre
    links = [u for u in links if "2025" in u or "2025" in u.lower()]
    if not links:
        return None
    return links[0]

def get_latest_pdf():
    url = pick_first_pdf(MICM_2025_URL)
    if not url:
        url = pick_first_pdf(MICM_FALLBACK_URL)
    if not url:
        raise RuntimeError("No se encontraron PDFs del MICM.")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return url, resp.content

def ocr_pdf(pdf_bytes: bytes, pages=(0,1), dpi=300, lang="spa+eng"):
    text_chunks = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        max_page = min(doc.page_count, pages[1]+1 if isinstance(pages, tuple) else doc.page_count)
        for i in range(pages[0], max_page):
            page = doc.load_page(i)
            # render a imagen
            pix = page.get_pixmap(dpi=dpi)
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            try:
                chunk = pytesseract.image_to_string(img, lang=lang)
            except Exception:
                # si no está el paquete de idioma ES, caemos a ENG
                chunk = pytesseract.image_to_string(img)
            text_chunks.append(chunk)
    return "\n".join(text_chunks)

def norm_key(label: str) -> str:
    k = label.lower().strip()
    for needle, norm in FUEL_KEYS.items():
        if needle in k:
            return norm
    return re.sub(r"[^a-z0-9]+", "_", k)

def find_official_block(text: str) -> str:
    tl = text.lower()
    idx = tl.find("precio oficial a pagar por el p")
    if idx == -1:
        # tolerar OCR con espacios/acentos raros
        idx = tl.find("precio oficial a pagar")
    if idx == -1:
        return ""
    block = text[idx: idx+8000]  # ventana suficiente
    # cortar donde empiezan otras secciones (paridad, estructura…)
    end = len(block)
    bl = block.lower()
    for mark in STOP_MARKERS:
        j = bl.find(mark)
        if j != -1:
            end = min(end, j)
    return block[:end]

NUM_RE = re.compile(r"(?<!\d)(\d{2,4}[.,]\d{2})(?!\d)")

def extract_prices_from_block(block: str):
    items = []
    lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
    # Para cada combustible conocido, busca en línea actual o vecinas el número final
    for name in FUEL_ORDER:
        label_idx = next((i for i, ln in enumerate(lines) if name in ln.lower()), None)
        if label_idx is None:
            continue
        window = "  ".join(lines[label_idx: label_idx+3])  # línea + dos siguientes
        m = None
        # tomar el ÚLTIMO número estilo 290.10 / 290,10 en la ventana
        matches = list(NUM_RE.finditer(window))
        if matches:
            m = matches[-1]
        if not m:
            continue
        raw = m.group(1)
        price = float(raw.replace(".", "").replace(",", ".")) if raw.count(",") == 1 and raw.count(".") > 1 else float(raw.replace(",", "."))
        unit = "m3" if "gas natural" in name else "galon"
        # construir label bonito
        pretty = name.title().replace("1%s","1%S").replace("Fueloil","Fueloil").replace("Oil","Oil")
        # normalizar excepciones
        if "gasoil optimo" in name: pretty = "Gasoil Óptimo"
        if "glp" == name: pretty = "Gas Licuado de Petróleo (GLP)"
        if "gas licuado" in name: pretty = "Gas Licuado de Petróleo (GLP)"
        if "fuel oil #6" in name or "fueloil #6" in name: pretty = "Fueloil #6"
        if "fuel oil 1%s" in name or "fueloil 1%s" in name: pretty = "Fueloil 1%S"
        items.append({
            "label": pretty,
            "key": norm_key(pretty),
            "price_dop": round(price, 2),
            "unit": unit,
            "change": None
        })
    # de-duplicar por key y mantener el primero (tabla oficial)
    dedup = {}
    for it in items:
        dedup.setdefault(it["key"], it)
    # ordenar similar a FUEL_ORDER
    order_map = {norm_key(n if n != "glp" else "Gas Licuado de Petróleo (GLP)"): i for i, n in enumerate(FUEL_ORDER)}
    out = list(dedup.values())
    out.sort(key=lambda x: order_map.get(x["key"], 999))
    return out

# (Opcional) Trata de sacar el rango de fechas
SPANISH_MONTHS = {
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,"noviembre":11,"diciembre":12
}
def parse_week(text: str):
    tl = text.lower()
    m = re.search(r"del\s+(\d{1,2}).{0,15}?al\s+(\d{1,2}).{0,15}?de\s+([a-záéíóúñ]+).{0,15}?de\s+(\d{4})", tl, re.IGNORECASE)
    if not m: 
        return None, None
    d1, d2, mon, year = m.groups()
    mon = mon.replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ñ","n")
    month = SPANISH_MONTHS.get(mon, None)
    if not month: 
        return None, None
    y = int(year)
    d1 = int(d1); d2 = int(d2)
    start = f"{y:04d}-{month:02d}-{d1:02d}"
    end   = f"{y:04d}-{month:02d}-{d2:02d}"
    return start, end

def main():
    ensure_dirs()
    pdf_url, pdf_bytes = get_latest_pdf()

    # OCR de las 1-2 primeras páginas (normalmente ahí está la tabla)
    raw_text = ocr_pdf(pdf_bytes, pages=(0,1), dpi=320, lang="spa+eng")

    block = find_official_block(raw_text)
    items = extract_prices_from_block(block) if block else []

    # como safety-net: si no encontró nada en el bloque, intenta en todo el texto (último recurso)
    if not items:
        items = extract_prices_from_block(raw_text)

    # semana (opcional)
    s, e = parse_week(raw_text)

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
