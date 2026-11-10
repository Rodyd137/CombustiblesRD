"""Lee los precios del aviso semanal del MICM desde la CAPA DE TEXTO del PDF.

Las cifras salen exactas (sin OCR). Cada fila de combustible líquido trae:
  paridad | ley 112-00 | ley 495-06 | distribuidor | detallista | transporte
  | precio oficial | ajuste | variación
y el precio al público es  oficial + ajuste  (el ajuste va entre paréntesis
cuando resta). La variación permite comprobar cada semana contra la anterior.
Las filas «EGP-C» / «EGP-T» son para generadoras eléctricas: se ignoran.
"""
import re

NUM = re.compile(r"\(?\d{1,3}(?:,\d{3})*\.\d{2}\)?")

# clave -> expresión que debe cumplir el INICIO de la línea (seguida de números)
FILAS = {
    "gasolina_premium": r"gasolina\s+premium",
    "gasolina_regular": r"gasolina\s+regular",
    "gasoil_regular":   r"gasoil\s+regular",
    "gasoil_optimo":    r"gasoil\s+[óo]ptimo",
    "avtur":            r"avtur",
    "kerosene":         r"kerosene",
    "fueloil_6":        r"fuel\s*oil",
    "fueloil_1s":       r"fuel\s*oil\s+1\s*%\s*azufre\s*\(fo6,\s*1\s*%\s*s\)",
}
CILINDROS = {
    "cilindro_100_lb": 100, "cilindro_50_lb": 50,
    "cilindro_25_lb": 25, "cilindro_15_lb": 15,
}


def _num(tok):
    neg = tok.startswith("(")
    v = float(tok.strip("()").replace(",", ""))
    return -v if neg else v


def leer(texto):
    """Devuelve {clave: {"price_dop": float, "variacion": float|None}}."""
    out = {}
    for linea in texto.splitlines():
        l = linea.strip()
        ll = l.lower()
        if "egp-" in ll or "egp " in ll:
            continue
        for clave, pat in FILAS.items():
            m = re.match(pat + r"\s+(?=\d)", ll)
            if not m or clave in out:
                continue
            nums = [_num(t) for t in NUM.findall(l[m.end():])]
            if len(nums) < 9:
                continue
            oficial, ajuste = nums[6], nums[7]
            publico = round(oficial + ajuste, 2)
            if len(nums) >= 10 and abs(nums[8] - publico) < 0.011:
                variacion = nums[9]      # la columna «precio al público» vino en la fila
            else:
                variacion = nums[8]
            out[clave] = {"price_dop": publico, "variacion": variacion}
        if ll.startswith("gas licuado") and "glp" not in out:
            nums = [_num(t) for t in NUM.findall(l)]
            if len(nums) >= 11:  # ... oficial, ajuste, bonogas, público, variación
                out["glp"] = {"price_dop": nums[9], "variacion": nums[10]}
        m = re.match(r"cilindros?\s+de\s+(\d+)\s+libras", ll)
        if m:
            nums = [_num(t) for t in NUM.findall(l[l.find(")") + 1:])]
            clave = f"cilindro_{m.group(1)}_lb"
            if nums and clave in CILINDROS and clave not in out:
                out[clave] = {"price_dop": nums[-1], "variacion": None}
    return out


def texto_de_pdf(pdf_bytes):
    """Capa de texto de las 2 primeras páginas (pypdf). '' si no tiene."""
    import io, logging
    from pypdf import PdfReader
    logging.getLogger("pypdf").setLevel(logging.ERROR)  # avisos de xref del MICM
    try:
        lector = PdfReader(io.BytesIO(pdf_bytes))
        return "\n".join((p.extract_text() or "") for p in lector.pages[:2])
    except Exception as e:
        print(f"⚠️  no pude leer el texto del PDF: {e}")
        return ""


# Lo que publica el feed (mismo juego de claves, etiquetas y unidades que
# daba el OCR, para que la app no vea filas nuevas).
SALIDA = [
    ("gasolina_premium", "Gasolina Premium", "galon"),
    ("gasolina_regular", "Gasolina Regular", "galon"),
    ("gasoil_regular",   "Gasoil Regular",   "galon"),
    ("gasoil_optimo",    "Gasoil Óptimo",    "galon"),
    ("avtur",            "Avtur",            "galon"),
    ("kerosene",         "Kerosene",         "galon"),
    ("fueloil_6",        "Fueloil #6",       "galon"),
    ("glp",              "Gas Licuado de Petróleo (GLP)", "galon"),
    ("cilindro_100_lb",  "Cilindro 100 lb",  "cilindro"),
    ("cilindro_50_lb",   "Cilindro 50 lb",   "cilindro"),
    ("cilindro_25_lb",   "Cilindro 25 lb",   "cilindro"),
    ("cilindro_15_lb",   "Cilindro 15 lb",   "cilindro"),
]
# Sin estas cuatro no damos por buena la lectura y se usa el OCR.
IMPRESCINDIBLES = {"gasolina_premium", "gasolina_regular", "gasoil_regular", "gasoil_optimo"}


def _cambio(variacion):
    if variacion is None:
        return None
    if variacion > 0:
        return {"type": "up", "amount_dop": round(variacion, 2)}
    if variacion < 0:
        return {"type": "down", "amount_dop": round(variacion, 2)}
    return {"type": "same", "amount_dop": 0.0}


def items_desde_texto(texto):
    """Lista de items con el formato del feed, o None si la lectura no sirve."""
    leidos = leer(texto or "")
    if not IMPRESCINDIBLES.issubset(leidos):
        return None
    items = []
    for clave, etiqueta, unidad in SALIDA:
        if clave in leidos:
            v = leidos[clave]
            items.append({"label": etiqueta, "key": clave, "price_dop": v["price_dop"],
                          "unit": unidad, "change": _cambio(v["variacion"])})
    return items
