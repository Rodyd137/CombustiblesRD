# CombustiblesRD

[![Scrape & Publish](https://github.com/Rodyd137/CombustiblesRD/actions/workflows/scrape.yml/badge.svg)](https://github.com/Rodyd137/CombustiblesRD/actions/workflows/scrape.yml)
[![GitHub Pages](https://img.shields.io/badge/data-live-brightgreen)](https://rodyd137.github.io/CombustiblesRD/data/latest.json)

Pipeline público que extrae los **precios semanales de combustibles** que el Ministerio de Industria, Comercio y Mipymes (MICM) de República Dominicana publica todos los **viernes**, y los expone como un feed JSON consumible por apps móviles y web.

> Usado por la app **Bartelo** en el módulo Combustible.

---

## 🔗 Endpoints

| Recurso | URL | Tamaño |
|---|---|---|
| **Precio actual** | `https://rodyd137.github.io/CombustiblesRD/data/latest.json` | ~1 KB |
| Tendencia completa | `https://rodyd137.github.io/CombustiblesRD/data/trend.json` | ~190 KB |
| Tendencia reducida | `https://rodyd137.github.io/CombustiblesRD/data/trend_min.json` | ~110 KB |
| Snapshot por fecha | `https://rodyd137.github.io/CombustiblesRD/data/history/{YYYY-MM-DD}.json` | ~1 KB c/u |

---

## 📄 Schema de `latest.json`

```jsonc
{
  "source": "https://micm.gob.do/.../aviso-semanal-...pdf",
  "updated_at_utc": "2026-05-30T10:03:47Z",
  "week": {
    "start_date": "2026-05-30",       // ISO date — inicio de vigencia
    "end_date":   "2026-06-05",       // ISO date — fin de vigencia
    "label":      "Vigente: 30 may – 5 jun 2026"
  },
  "week_label":    "Vigente: 30 may – 5 jun 2026",  // espejo del label (compat)
  "currency": "DOP",
  "items": [
    {
      "label":     "Gasolina Regular",
      "key":       "gasolina_regular",
      "price_dop": 307.50,
      "unit":      "galon",            // "galon" o "m3" (gas natural)
      "change": {
        "type":       "up",            // "up" | "down" | "same"
        "amount_dop": 7.50             // delta vs semana anterior
      }
    }
    // … hasta 10 productos
  ]
}
```

**Productos soportados** (keys canónicas): `gasolina_premium`, `gasolina_regular`, `gasoil_regular`, `gasoil_optimo`, `avtur`, `kerosene`, `fueloil_6`, `fueloil_1s`, `glp`, `gas_natural`.

---

## ⚙️ Funcionamiento

```
┌───────────────────────┐
│ GitHub Actions cron   │  Viernes 8AM–11PM RD (cada 15 min)
│   .github/workflows/  │  + verificación diaria Lun–Jue
│        scrape.yml     │
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│ scraper/scraper.py    │  1. Descarga el último PDF del MICM
│   - PyMuPDF + Tesseract│  2. OCR posicional sobre las primeras 2 páginas
│   - python-dateutil   │  3. Extrae items + semana de vigencia
└──────────┬────────────┘  4. Compara con latest.json publicado → `change`
           │               5. Salida temprana si nada cambió (no commit)
           ▼
┌───────────────────────┐
│ data/latest.json      │  Snapshot vigente
│ data/history/         │  Snapshots históricos (1 por día publicado)
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│ build_trend.py        │  Reconstruye trend.json + trend_min.json
└──────────┬────────────┘
           │
           ▼
┌───────────────────────┐
│ GitHub Pages          │  Sirve los JSON públicamente
└───────────────────────┘
```

### Cron schedule

El MICM publica los **viernes** sin hora exacta. El workflow corre:

| Cuándo | Frecuencia |
|---|---|
| Viernes 8 AM – 7 PM RD (12 – 23 UTC) | cada 15 min |
| Viernes 7 PM – 11 PM RD (00 – 03 UTC del sábado) | cada 15 min |
| Lun – Jue, 9 PM RD (01 UTC) | 1 vez al día (verificación) |

**~70 runs/semana** (vs ~336 con el cron antiguo cada 30 min).

### Salvaguardas

- **No publica feeds incompletos**: si el OCR devuelve menos de 5 productos, conserva el último snapshot bueno y registra un warning.
- **No commitea si nada cambió**: hash del set de items + ventana de vigencia se compara con el publicado anterior.
- **Delta vs semana previa**: cada item incluye `change` (sube/baja/igual + monto).

---

## 🧪 Desarrollo local

```bash
git clone https://github.com/Rodyd137/CombustiblesRD.git
cd CombustiblesRD

# Dependencias del sistema (OCR)
brew install tesseract             # macOS
# sudo apt install tesseract-ocr   # Debian/Ubuntu

# Python deps
pip install -r scraper/requirements.txt

# Ejecuta el scraper (descarga el PDF más reciente, hace OCR, escribe data/)
python scraper/scraper.py

# Reconstruye agregados
python scraper/build_trend.py
python scraper/build_trend_min.py
```

---

## 📜 Fuente oficial

- Página principal del MICM: [Avisos semanales de precios de combustibles](https://micm.gob.do/direcciones/combustibles/avisos-semanales-de-precios/avisos-semanales-de-precios-de-combustibles/)

Este repositorio NO está afiliado al MICM. Los datos se procesan automáticamente desde los PDFs públicos para fines informativos.
