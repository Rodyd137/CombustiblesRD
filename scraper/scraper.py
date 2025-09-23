# scraper/scraper.py
import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pdfplumber

MICM_LIST_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

# -------------------------
# Descargar último PDF
# -------------------------
def get_latest_pdf():
    r = requests.get(MICM_LIST_URL, timeout=40)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    pdfs = [urljoin(MICM_LIST_URL, a["href"])
            for a in soup.select("a[href$='.pdf']")]
    if not pdfs:
        raise RuntimeError("No se encontraron PDFs en el MICM")

    pdf_url = pdfs[0]  # el primero normalmente es el más reciente
    pdf = requests.get(pdf_url, timeout=60)
    pdf.raise_for_status()
    return pdf_url, pdf.content

# -------------------------
# Parsear tabla oficial
# -------------------------
def parse_official_prices(pdf_bytes: bytes):
    items = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    # row es una lista de celdas
                    line = " ".join([c or "" for c in row])
                    if not line.strip():
                        continue

                    # Solo nos quedamos con filas de la tabla oficial
                    if "gasolina" in line.lower() or "gasoil" in line.lower() or "glp" in line.lower() \
                       or "kerosene" in line.lower() or "avtur" in line.lower() or "fuel" in line.lower():
                        parts = [c for c in row if c]
                        if len(parts) >= 2:
                            label = parts[0].strip()
                            price_txt = parts[1].strip()

                            # Normalizar número
                            price_txt = price_txt.replace("RD$", "").replace(",", ".")
                            try:
                                price = float(re.sub(r"[^\d.]", "", price_txt))
                            except:
                                continue

                            unit = "galon"
                            if "gas natural" in label.lower():
                                unit = "m3"

                            items.append({
                                "label": label,
                                "key": re.sub(r"[^a-z0-9]+", "_", label.lower()),
                                "price_dop": price,
                                "unit": unit,
                                "change": None
                            })
    return items

# -------------------------
# MAIN
# -------------------------
def main():
    ensure_dirs()

    pdf_url, pdf_bytes = get_latest_pdf()
    items = parse_official_prices(pdf_bytes)

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
