"""Recorre las listas anuales de avisos del MICM y lee cada circular con la
capa de texto (texto_pdf). Solo escribe un INFORME (data/backfill/informe.json);
no toca el historial: el historial se escribe después de revisarlo.
Uso único (10-nov-2026) para rellenar el historial largo y los huecos.
"""
import os, json, time, re, requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from texto_pdf import texto_de_pdf, leer
import scraper

BASE = ("https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/"
        "avisos-semanales-de-precios-de-combustibles/")
PAGINAS = [BASE] + [f"{BASE}avisos-semanales-de-precios-de-combustibles-{y}/" for y in range(2021, 2027)]
OUT = os.path.join(scraper.OUT_DIR, "backfill")


def enlaces(pagina):
    try:
        r = requests.get(pagina, timeout=40)
        print(f"{r.status_code} {pagina}")
        if r.status_code != 200:
            return []
        sopa = BeautifulSoup(r.text, "lxml")
        return [urljoin(pagina, a["href"]) for a in sopa.select("a[href$='.pdf'], a[href$='.PDF']")]
    except Exception as e:
        print(f"⚠️  {pagina}: {e}")
        return []


def main():
    os.makedirs(OUT, exist_ok=True)
    urls = []
    for p in PAGINAS:
        for u in enlaces(p):
            if u not in urls:
                urls.append(u)
    print(f"{len(urls)} PDFs en las listas")
    informe = []
    for u in urls:
        fila = {"url": u}
        try:
            r = requests.get(u, timeout=60)
            r.raise_for_status()
            texto = texto_de_pdf(r.content)
            fila["con_texto"] = len(texto) > 500
            fila["precios"] = leer(texto)
            s, e = scraper.parse_week_from_pdf_url(u)
            if not (s and e):
                lineas = [{"text": l, "text_l": l.lower()} for l in texto.splitlines()]
                s, e = scraper.parse_week_from_lines(lineas)
            fila["desde"], fila["hasta"] = s, e
            m = re.search(r"dado[a]?\s+en.*?el\s+d[ií]a\s+(.{0,120})", texto, re.I | re.S)
            fila["dada"] = m.group(1)[:120] if m else None
        except Exception as e:
            fila["error"] = str(e)
        informe.append(fila)
        time.sleep(0.5)
    with open(os.path.join(OUT, "informe.json"), "w", encoding="utf-8") as f:
        json.dump(informe, f, ensure_ascii=False, indent=1)
    ok = sum(1 for x in informe if x.get("precios"))
    print(f"✅ {ok}/{len(informe)} circulares leídas")


if __name__ == "__main__":
    main()
