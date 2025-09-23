# scraper/scraper.py
import os, re, io, json, datetime, requests, unicodedata
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pdfplumber

MICM_LIST_URL = "https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/"

ROOT = os.path.dirname(__file__)
OUT_DIR = os.path.abspath(os.path.join(ROOT, "..", "data"))
HIST_DIR = os.path.join(OUT_DIR, "history")

# -------------------------
# Normalización / utilidades
# -------------------------
def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

def collapse(s: str) -> str:
    # minúsculas, sin acentos, solo a-z0-9
    s = strip_accents(s).lower()
    return re.sub(r"[^a-z0-9]", "", s)

def parse_num(s: str) -> float:
    s = s.strip()
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    return float(s)

def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(HIST_DIR, exist_ok=True)

# -------------------------
# Etiquetas y claves
# -------------------------
LABEL_CANON = {
    "gasolina premium": "Gasolina Premium",
    "gasolina regular": "Gasolina Regular",
    "gasoil premium":   "Gasoil Premium",
    "gasoil regular":   "Gasoil Regular",
    "gasoil optimo":    "Gasoil Óptimo",
    "gasoil óptimo":    "Gasoil Óptimo",
    "avtur":            "Avtur",
    "kerosene":         "Kerosene",
    "fuel oil #6":      "Fuel Oil #6",
    "fueloil #6":       "Fuel Oil #6",
    "fuel oil 1%s":     "Fuel Oil 1%S",
    "fueloil 1%s":      "Fuel Oil 1%S",
    "gas licuado de petroleo": "Gas Licuado de Petróleo",
    "gas licuado de petróleo": "Gas Licuado de Petróleo",
    "gas natural":      "Gas Natural",
}
def norm_key(label: str) -> str:
    low = label.lower()
    if "gasolina" in low and "premium" in low: return "gasolina_premium"
    if "gasolina" in low and "regular" in low: return "gasolina_regular"
    if "gasoil" in low and "premium" in low:   return "gasoil_premium"
    if "gasoil" in low and "regular" in low:   return "gasoil_regular"
    if "gasoil" in low and ("óptimo" in low or "optimo" in low): return "gasoil_optimo"
    if "avtur" in low: return "avtur"
    if "keros" in low: return "kerosene"
    if ("fuel" in low or "fueloil" in low) and ("1" in low and "s" in low): return "fueloil_1s"
    if ("fuel" in low or "fueloil" in low) and ("#6" in low or " 6" in low): return "fueloil_6"
    if "gas licuado" in low or "glp" in low: return "glp"
    if "gas natural" in low: return "gas_natural"
    return re.sub(r"[^a-z0-9]+", "_", low).strip("_")

# Variantes para reconocer cada fila de la tabla oficial
LABEL_VARIANTS = {
    "Gasolina Premium":   [r"\bgasolina\s+premium\b"],
    "Gasolina Regular":   [r"\bgasolina\s+regular\b"],
    "Gasoil Premium":     [r"\bgasoil\s+premium\b"],
    "Gasoil Regular":     [r"\bgasoil\s+regular\b"],
    "Gasoil Óptimo":      [r"\bgasoil\s+o?́?ptimo\b", r"\bgasoil\s+optimo\b"],
    "Avtur":              [r"\bavtur\b"],
    "Kerosene":           [r"\bkeros[eé]ne\b", r"\bkerosene\b"],
    "Fuel Oil #6":        [r"\bfuel\s*oil\s*#?6\b", r"\bfueloil\s*#?6\b", r"\bfuel\s*oil\s*n?°?\s*6\b"],
    "Fuel Oil 1%S":       [r"\bfuel\s*oil\s*1\s*%?\s*s\b", r"\bfueloil\s*1\s*%?\s*s\b"],
    "Gas Licuado de Petróleo": [r"\bgas\s+licuado\s+de\s+petr[oó]leo(?:\s*\(glp\))?\b", r"\bglp\b"],
    "Gas Natural":        [r"\bgas\s+natural\b"],
}
NUM_PAT = r"(?:RD\$\s*)?([\d\.\,]+)"

# -------------------------
# Descargar PDF más reciente
# -------------------------
def get_latest_pdf():
    r = requests.get(MICM_LIST_URL, timeout=40, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    candidates = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.lower().endswith(".pdf"):
            url = urljoin(MICM_LIST_URL, href)
            txt = a.get_text(" ", strip=True) or ""
            score = 0
            low = strip_accents(txt).lower()
            if "aviso" in low or "combustibl" in low or "precio" in low:
                score += 5
            if "sem" in low or "semana" in low:
                score += 1
            candidates.append((score, url, txt))
    if not candidates:
        raise RuntimeError("No se encontró PDF del MICM.")
    candidates.sort(key=lambda x: x[0], reverse=True)
    url, link_text = candidates[0][1], candidates[0][2]

    pdf = requests.get(url, timeout=60, headers={"User-Agent": "CombustiblesRDBot/1.0"})
    pdf.raise_for_status()
    return url, link_text, pdf.content

# -------------------------
# Extraer texto por página
# -------------------------
def extract_pages_text(pdf_bytes: bytes) -> list[str]:
    texts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for p in pdf.pages:
            try:
                txt = p.extract_text(x_tolerance=2, y_tolerance=2) or ""
            except Exception:
                txt = p.extract_text() or ""
            texts.append(txt)
    return texts

# -------------------------
# Encontrar el bloque: "PRECIO OFICIAL A PAGAR POR EL PUBLICO (RD$/GL)"
# -------------------------
def find_official_public_section(page_texts: list[str]) -> list[tuple[int, str]]:
    """
    Devuelve lista de (page_index, texto_bloque) para cada página donde
    esté el encabezado 'PRECIO OFICIAL A PAGAR POR EL PUBLICO (RD$/GL)'.
    Corta el bloque hasta antes de 'PARIDAD'/'PARIDAD DE IMPORTACION'/'REFERENCIA'.
    """
    targets = []
    for i, raw in enumerate(page_texts):
        norm = strip_accents(raw).lower()
        col = collapse(raw)

        # Encabezado robusto (con/ sin saltos)
        # variantes aceptadas: preciooficialapagarporelpúblico / ...publico / (rd$/gl) opcional
        ok_head = (
            "preciooficialapagarporelpublico" in col or
            "preciooficialapagarporelpublico" in col  # por si falta una 'l'
        )
        if not ok_head:
            # fallback: componentes cerca
            ok_head = ("precio" in norm and "oficial" in norm and "publico" in norm and "pagar" in norm)

        if not ok_head:
            continue

        # corta el bloque desde el encabezado hasta antes de paridad/referencia/nota
        start_idx = norm.find("precio oficial a pagar")
        if start_idx == -1:
            # intenta con colapsado
            start_idx = col.find("preciooficialapagarporelpublico")
            if start_idx != -1:
                # aproximación al índice en texto crudo: usa todo el texto desde arriba
                start_idx = 0

        end_idx = len(raw)
        for term in ["paridad", "referencia", "nota:", "paridad de importacion", "precios de paridad"]:
            pos = norm.find(term, (start_idx if start_idx > 0 else 0) + 1)
            if pos != -1:
                end_idx = min(end_idx, pos)

        block = raw[start_idx:end_idx] if start_idx != -1 else raw
        # si el bloque luce razonable (contiene al menos una etiqueta conocida)
        if any(re.search(v[0], block, flags=re.IGNORECASE) for v in LABEL_VARIANTS.values()):
            targets.append((i, block))
        else:
            # igual añadimos para intentar parseo por ventana
            targets.append((i, raw))
    return targets

# -------------------------
# Parsear filas (label + precio) dentro del bloque oficial
# -------------------------
def parse_items_from_blocks(blocks: list[tuple[int, str]]) -> list[dict]:
    items = []
    flags = re.IGNORECASE | re.DOTALL

    def add(label: str, price: str):
        try:
            val = parse_num(price)
        except Exception:
            return
        unit = "m3" if label.lower() == "gas natural" else "galon"
        items.append({
            "label": label,
            "key": norm_key(label),
            "price_dop": round(val, 2),
            "unit": unit,
            "change": None
        })

    for _, block in blocks:
        # trabajamos por líneas, usando ventanas cortas (para PDFs “rotos”)
        lines = [ln.strip() for ln in block.splitlines() if ln.strip()]
        for i in range(len(lines)):
            # Ventana: línea i + siguientes dos
            chunk = " ".join(lines[i:i+3])
            chunk = re.sub(r"[ \t]+", " ", chunk)

            for canon_label, variants in LABEL_VARIANTS.items():
                matched = False
                for v in variants:
                    m = re.search(v, chunk, flags)
                    if not m:
                        continue

                    # Busca número cerca (después o antes)
                    after = chunk[m.end():m.end()+140]
                    before = chunk[max(0, m.start()-100):m.start()]
                    mnum = re.search(NUM_PAT, after, flags) or re.search(NUM_PAT, before, flags)
                    if mnum:
                        add(canon_label, mnum.group(1))
                        matched = True
                        break
                if matched:
                    break

    # dedup por key
    out, seen = [], set()
    for it in items:
        if it["key"] in seen:
            continue
        seen.add(it["key"])
        out.append(it)
    return out

# -------------------------
# Extra: rango de semana (best-effort)
# -------------------------
SPANISH_MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12
}
RANGE_RE = re.compile(
    r"(?:semana\s+)?del\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:al|-)\s+(\d{1,2})\s+de\s+([a-záéíóú]+)\s+de\s+(\d{4})",
    re.IGNORECASE
)

def parse_week_from_text(text: str):
    t = strip_accents(text).lower()
    m = RANGE_RE.search(t)
    if not m:
        return None, None
    d1, m1, d2, m2, y = m.groups()
    y = int(y)
    m1n = SPANISH_MONTHS.get(m1, None)
    m2n = SPANISH_MONTHS.get(m2, None)
    if not (m1n and m2n): return None, None
    try:
        s = datetime.date(y, m1n, int(d1))
        e = datetime.date(y, m2n, int(d2))
        return s.isoformat(), e.isoformat()
    except Exception:
        return None, None

def parse_week(page_texts: list[str], pdf_url: str, link_text: str):
    all_text = "\n".join(page_texts)
    s, e = parse_week_from_text(all_text)
    if s and e: return s, e
    fname = os.path.basename(pdf_url)
    s, e = parse_week_from_text(fname)
    if s and e: return s, e
    s, e = parse_week_from_text(link_text or "")
    return s, e

# -------------------------
# MAIN
# -------------------------
def main():
    ensure_dirs()

    pdf_url, link_text, pdf_bytes = get_latest_pdf()
    page_texts = extract_pages_text(pdf_bytes)

    # 1) Encuentra secciones “PRECIO OFICIAL A PAGAR POR EL PUBLICO (RD$/GL)”
    blocks = find_official_public_section(page_texts)

    # 2) Parsea SOLO de esos bloques
    items = parse_items_from_blocks(blocks) if blocks else []

    # 3) Si no hay nada, mejor vacio que paridad
    if not items:
        items = []

    # 4) Semana (si se puede)
    start_date, end_date = parse_week(page_texts, pdf_url, link_text)

    payload = {
        "source": pdf_url,
        "updated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "week": {"start_date": start_date, "end_date": end_date},
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
