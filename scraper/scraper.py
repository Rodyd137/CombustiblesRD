import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pdfplumber

MICM_LIST_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

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
    "gas licuado de petróleo": "glp",
    "gas licuado de petroleo": "glp",
    "gas natural": "gas_natural",
}

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

def get_latest_pdf():
    r = requests.get(MICM_LIST_URL, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    pdfs = [urljoin(MICM_LIST_URL, a["href"]) for a in soup.select("a[href$='.pdf']")]
    if not pdfs:
        raise RuntimeError("No se encontraron PDFs en la página del MICM")
    url = pdfs[0]
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return url, resp.content

def norm_key(label: str) -> str:
    k = label.lower().strip()
    for needle, norm in FUEL_KEYS.items():
        if needle in k:
            return norm
    return re.sub(r"[^a-z0-9]+", "_", k)

def parse_official_block(pdf_bytes: bytes):
    items = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    # Busca bloque "PRECIO OFICIAL A PAGAR POR EL PUBLICO"
    block_match = re.search(r"PRECIO\s+OFICIAL\s+A\s+PAGAR\s+POR\s+EL\s+P[ÚU]BLICO.*", full_text, re.IGNORECASE | re.DOTALL)
    if not block_match:
        return []
    block = block_match.group(0)
    # Busca líneas con: nombre + número
    line_re = re.compile(r"(Gasolina Premium|Gasolina Regular|Gasoil Regular|Gasoil Óptimo|Gasoil Optimo|Avtur|Kerosene|Fuel.?Oil.?#?6|Fuel.?Oil.?1%S|GLP|Gas Natural)[^\d]+([\d,.]+)", re.IGNORECASE)
    for m in line_re.finditer(block):
        label = m.group(1).strip()
        price_str = m.group(2).replace(",", ".")
        try:
            price = float(re.sub(r"[^\d.]", "", price_str))
        except:
            continue
        unit = "m3" if "natural" in label.lower() else "galon"
        items.append({
            "label": label,
            "key": norm_key(label),
            "price_dop": round(price, 2),
            "unit": unit,
            "change": None
        })
    return items

def main():
    ensure_dirs()
    pdf_url, pdf_bytes = get_latest_pdf()
    items = parse_official_block(pdf_bytes)
    payload = {
        "source": pdf_url,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {"start_date": None, "end_date": None},
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
