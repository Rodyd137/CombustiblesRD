# scraper/micm_scraper.py
import os, re, io, json, datetime, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pdfplumber
from dateutil import parser as dtp

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
    "fueloil #6": "fueloil_6",
    "fuel oil #6": "fueloil_6",
    "fueloil 1%s": "fueloil_1s",
    "fuel oil 1%s": "fueloil_1s",
    "gas licuado de petróleo": "glp",
    "gas licuado de petroleo": "glp",
    "gas licuado de petróleo (glp)": "glp",
    "gas licuado de petroleo (glp)": "glp",
    "gas natural": "gas_natural",
}

DEC_RE = re.compile(r"([\d\.\,]+)")  # números con . y ,

# “Semana del 6 al 12 de septiembre de 2025” (variantes)
RANGE_RE = re.compile(
    r"semana\s+del\s+([\d]{1,2}\s+de\s+\w+|\d{1,2})\s*(?:al|-)\s*([\d]{1,2}\s+de\s+\w+|\d{1,2})\s+de\s+(\d{4})",
    re.IGNORECASE
)

def parse_num(s: str) -> float:
    # Maneja 290.10 y 290,10 y 29,010.00 (convierte a 290.10)
    s = s.strip()
    # Si hay puntos como miles y coma como decimal -> quita puntos, cambia coma a punto
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        # Si solo hay comas, asúmelas como decimales
        s = s.replace(",", ".")
    return float(s)

def norm_key(label: str) -> str:
    k = label.lower()
    k = re.sub(r"\s+", " ", k)
    for needle, norm in FUEL_KEYS.items():
        nn = needle.replace("%s", "%s")  # mantener % en patrón
        if "%s" in needle:
            # coincidir "1%s" con "1%s" o "1 % s" variantes
            if "1" in k and "s" in k and "fuel" in k or "fueloil" in k:
                return norm
        if nn in k:
            return norm
    return re.sub(r"[^a-z0-9]+", "_", k).strip("_")

def get_latest_pdf_url() -> str:
    r = requests.get(MICM_LIST_URL, timeout=30, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    # Busca el primer enlace a PDF dentro del contenido principal
    # (la página del MICM suele listar los “Avisos semanales…” como enlaces a PDF)
    candidates = []
    for a in soup.select("a[href]"):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            url = urljoin(MICM_LIST_URL, href)
            # Intenta priorizar el que mencione “aviso” o “combustible”
            score = 0
            txt = (a.get_text(" ", strip=True) or "").lower()
            if "aviso" in txt or "combustible" in txt or "precios" in txt:
                score += 5
            # algunas veces el más reciente aparece primero
            candidates.append((score, url))

    if not candidates:
        raise RuntimeError("No se encontró PDF en el listado del MICM")

    # mayor score primero; si empatan, usa el orden natural
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def extract_text_from_pdf(url: str) -> str:
    r = requests.get(url, timeout=60, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    with pdfplumber.open(io.BytesIO(r.content)) as pdf:
        pages = [p.extract_text() or "" for p in pdf.pages]
    text = "\n".join(pages)
    # normaliza espacios
    text = re.sub(r"[ \t]+", " ", text)
    return text

def parse_week(text: str):
    # Intenta detectar el rango “semana del … al … de 20XX”
    m = RANGE_RE.search(text)
    if not m:
        return None, None
    a, b, year = m.groups()
    year = int(year)
    def mk(s, default_month=None):
        s = s.strip()
        try:
            if "de" in s:
                return dtp.parse(f"{s} {year}", dayfirst=True)
            # viene solo “6” -> usa el mes de B
            if default_month:
                return dtp.parse(f"{s} {default_month.month} {year}", dayfirst=True)
        except Exception:
            return None
        return None
    end = mk(b)
    start = mk(a, default_month=end) if end else mk(a)
    return (start.date().isoformat() if start else None,
            end.date().isoformat() if end else None)

def parse_prices(text: str):
    """
    El PDF del MICM suele tener renglones tipo:
    GASOLINA PREMIUM RD$ 290.10 / GALÓN
    o variantes con puntuación. Vamos a buscar por etiqueta y cifra.
    """
    items = []
    lowered = text.lower()

    # Posibles etiquetas a buscar (en orden “habitual”)
    labels = [
        "Gasolina Premium",
        "Gasolina Regular",
        "Gasoil Regular",
        "Gasoil Óptimo",
        "Avtur",
        "Kerosene",
        "Fuel Oil #6",
        "Fuel Oil 1%S",
        "Gas Licuado de Petróleo",
        "Gas Natural",
    ]

    # Para cada etiqueta, encuentra el bloque cercano y extrae la primera cifra válida
    for label in labels:
        lab_low = label.lower()
        pos = lowered.find(lab_low)
        if pos == -1:
            continue
        window = text[pos: pos + 200]  # 200 chars hacia adelante suele ser suficiente
        m = DEC_RE.search(window)
        if not m:
            continue
        price = parse_num(m.group(1))
        # El MICM publica por galón excepto Gas Natural (m³)
        unit = "m3" if "natural" in lab_low else "galon"
        items.append({
            "label": label,
            "key": norm_key(label),
            "price_dop": round(price, 2),
            "unit": unit,
            "change": None  # El PDF oficial no siempre trae “sube/baja” como cifra
        })

    return items

def main():
    pdf_url = get_latest_pdf_url()
    pdf_text = extract_text_from_pdf(pdf_url)

    start_date, end_date = parse_week(pdf_text)
    items = parse_prices(pdf_text)

    payload = {
        "source": pdf_url,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {
            "start_date": start_date,
            "end_date": end_date
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
    main()
