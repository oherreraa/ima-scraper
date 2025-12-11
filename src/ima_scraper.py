#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IMA 8UIT Scraper

Objetivo:
- Recorrer las páginas:
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---1.html
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---2.html
    ...

- De cada convocatoria:
    · numero_convocatoria (texto completo, ej. 'SOLICITUD DE COTIZACION N° 4017-2025')
    · numero (ej. '4017-2025')
    · descripcion
    · publicado_el (dd/mm/yyyy)
    · tipo (BIENES / SERVICIO)
    · fecha_limite (dd/mm/yyyy)
    · hora_limite (hh:mm AM/PM)
    · estado (VIGENTE / VENCIDO, etc.) – solo se guardan VIGENTE
    · pagina_origen (número de página IMA)
    · tdr_url (URL PDF)
    · tdr_filename
    · tdr_downloaded (bool)
    · caracteristicas_tecnicas (bloque extraído)
    · caracteristicas_tecnicas_ocr (True si se usó OCR)

- Generar un único JSON:
    data/convocatorias_vigentes.json
"""

# ============================================================
# BLOQUE 1: Imports, constantes y logging
# ============================================================

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pytesseract
from PyPDF2 import PdfReader
from pdf2image import convert_from_path
from urllib.parse import urljoin

BASE_URL = "https://www.ima.org.pe/adquisiciones-bienes-servicios-v2"
START_PAGE = 1
MAX_PAGES = 30  # límite de seguridad

ROOT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_JSON = ROOT_DIR / "data" / "convocatorias_vigentes.json"
PDF_DIR = ROOT_DIR / "data" / "pdfs_ima"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# BLOQUE 2: Utilidades PDF / OCR
# ============================================================

def _extract_caracteristicas_block(full_text: str) -> Optional[str]:
    """
    Busca el bloque 'CARACTERISTICAS TECNICAS' dentro del texto completo
    (texto extraído del PDF, ya sea embebido u OCR).
    """
    if not full_text:
        return None

    normalized = full_text.upper()

    patterns = [
        "CARACTERISTICAS TECNICAS",
        "CARACTERÍSTICAS TÉCNICAS",
        "CARACTERISTICAS TÉCNICAS",
        "CARACTERÍSTICAS TECNICAS",
    ]

    start_idx = -1
    chosen = ""
    for p in patterns:
        pos = normalized.find(p)
        if pos != -1:
            start_idx = pos
            chosen = p
            break

    if start_idx == -1:
        return None

    # Posibles encabezados que marcan el final del bloque
    end_markers = [
        "CONDICIONES GENERALES",
        "CONDICIONES CONTRACTUALES",
        "CONDICIONES",
        "REQUISITOS",
        "OBLIGACIONES",
        "PLAZO DE ENTREGA",
        "PLAZO DE EJECUCION",
        "PLAZO DE EJECUCIÓN",
        "GARANTIAS",
        "GARANTÍAS",
        "FORMA DE PAGO",
    ]

    end_idx = len(full_text)
    for m in end_markers:
        pos = normalized.find(m, start_idx + len(chosen))
        if pos != -1 and pos > start_idx:
            end_idx = min(end_idx, pos)

    segment = full_text[start_idx:end_idx].strip()

    # Cortamos muy largos para evitar JSON gigantes
    max_len = 4000
    if len(segment) > max_len:
        segment = segment[:max_len] + "\n[...]"

    return segment


def extract_caracteristicas_from_pdf(
    pdf_path: str,
    enable_ocr_fallback: bool = True,
) -> Tuple[Optional[str], bool]:
    """
    Intenta extraer 'CARACTERISTICAS TECNICAS' de un PDF.

    Intento 1: texto embebido (PyPDF2).
    Intento 2 (fallback, si enable_ocr_fallback=True): OCR de imágenes
        (pdf2image + pytesseract).

    Devuelve:
      (bloque_texto_o_None, used_ocr)
    """
    text_pages: List[str] = []
    used_ocr: bool = False

    # ---------- Intento 1: Texto embebido ----------
    try:
        reader = PdfReader(pdf_path)
    except Exception as e:
        logging.warning("No se pudo abrir PDF '%s': %s", pdf_path, e)
        return None, False

    for page in reader.pages:
        try:
            t = page.extract_text() or ""
        except Exception:
            t = ""
        if t.strip():
            text_pages.append(t)

    if text_pages:
        full_text = "\n".join(text_pages)
        block = _extract_caracteristicas_block(full_text)
        if block:
            logging.info(
                "Bloque 'CARACTERISTICAS TECNICAS' obtenido desde texto embebido: %s",
                pdf_path,
            )
            return block, False

    # ---------- Intento 2: OCR ----------
    if not enable_ocr_fallback:
        return None, False

    try:
        images = convert_from_path(pdf_path, dpi=300)
    except Exception as e:
        logging.warning(
            "Fallback OCR: no se pudo rasterizar '%s' a imágenes: %s", pdf_path, e
        )
        return None, False

    used_ocr = True
    ocr_texts: List[str] = []

    for idx, img in enumerate(images):
        try:
            gray = img.convert("L")
            txt = pytesseract.image_to_string(gray)
            if txt.strip():
                ocr_texts.append(txt)
        except Exception as e:
            logging.warning("Error OCR en página %s de '%s': %s", idx, pdf_path, e)

    if not ocr_texts:
        logging.warning("Fallback OCR: no se obtuvo texto para '%s'", pdf_path)
        return None, used_ocr

    full_ocr_text = "\n".join(ocr_texts)
    block_ocr = _extract_caracteristicas_block(full_ocr_text)
    if block_ocr:
        logging.info(
            "Bloque 'CARACTERISTICAS TECNICAS' obtenido por OCR: %s", pdf_path
        )
    else:
        logging.info(
            "Fallback OCR: sin bloque 'CARACTERISTICAS TECNICAS' en '%s'", pdf_path
        )

    return block_ocr, used_ocr


def download_pdf(session: requests.Session, url: str, dest_dir: Path) -> Optional[Path]:
    """
    Descarga un PDF y lo guarda en dest_dir.
    Devuelve la ruta local o None si falla.
    """
    try:
        logging.info("Descargando PDF: %s", url)
        resp = session.get(url, timeout=60)
        if resp.status_code == 404:
            logging.warning("PDF 404: %s", url)
            return None
        resp.raise_for_status()
    except requests.RequestException as exc:
        logging.warning("Error descargando PDF %s: %s", url, exc)
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    filename = url.split("/")[-1] or "tdr_ima.pdf"
    path = dest_dir / filename

    try:
        with path.open("wb") as f:
            f.write(resp.content)
    except Exception as exc:
        logging.warning("Error guardando PDF '%s': %s", path, exc)
        return None

    return path


# ============================================================
# BLOQUE 3: Utilidades de parsing de HTML
# ============================================================

def build_page_url(page: int) -> str:
    return f"{BASE_URL}/s---{page}.html"


def get_html(session: requests.Session, url: str) -> Optional[str]:
    try:
        logging.info("Descargando página: %s", url)
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            logging.info("Página 404: %s", url)
            return None
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding
        return resp.text
    except requests.RequestException as exc:
        logging.error("Error al descargar %s: %s", url, exc)
        return None


def parse_page_convocatorias(
    session: requests.Session,
    html: str,
    page_number: int,
) -> List[Dict]:
    """
    Parsea una página HTML del IMA y devuelve las convocatorias VIGENTES,
    enriquecidas con la info del PDF (si existe).

    Estrategia:
      - Tomar el texto entre "CONVOCATORIAS VIGENTES" y "Anterior".
      - Trabajar línea por línea.
      - Cada bloque comienza con "SOLICITUD DE COTIZACION N°".
      - Dentro del bloque se detectan:
          · descripción
          · publicado_el
          · tipo (BIENES / SERVICIO)
          · fecha límite
          · hora límite
          · estado (VIGENTE / VENCIDO)
      - Se filtran solo las que tienen estado VIGENTE.
      - Los enlaces PDF se obtienen de los <a href="convmc_v2-files/...pdf">,
        en el mismo orden que las convocatorias de la página.
    """
    soup = BeautifulSoup(html, "html.parser")

    # ---------- 1) Segmento de texto útil ----------
    full_text = soup.get_text("\n", strip=True)

    if "CONVOCATORIAS VIGENTES" not in full_text:
        logging.info(
            "No se encontró 'CONVOCATORIAS VIGENTES' en la página %s", page_number
        )
        return []

    start_idx = full_text.index("CONVOCATORIAS VIGENTES")
    end_idx = full_text.find("Anterior", start_idx)
    if end_idx == -1:
        end_idx = len(full_text)

    segment = full_text[start_idx:end_idx]
    lines_raw = segment.split("\n")
    lines = [l.strip() for l in lines_raw if l.strip()]

    # Frases fijas que queremos eliminar
    header_phrases = {
        "CONVOCATORIAS VIGENTES",
        "PROVEEDORES CON BUENA PRO",
        "TIPO COTIZACION BUSCAR",
        "[ SELECCIONE ]  BIENES SERVICIO",
        "DESCRIPCION  TIPO PLAZO DESCARGAR",
        "DESCRIPCIÓN  TIPO PLAZO DESCARGAR",
    }
    header_upper = {h.upper() for h in header_phrases}
    cleaned_lines: List[str] = [
        l for l in lines if l.upper() not in header_upper
    ]

    # ---------- 2) Identificar bloques por SOLICITUD DE COTIZACION N° ----------
    indices = [
        i for i, l in enumerate(cleaned_lines)
        if "SOLICITUD DE COTIZACION N°" in l.upper()
    ]

    if not indices:
        logging.info(
            "Página %s: no se encontraron 'SOLICITUD DE COTIZACION N°'",
            page_number,
        )
        return []

    # ---------- 3) Extraer PDFs en orden ----------
    pdf_links = [
        urljoin(BASE_URL + "/", a["href"])
        for a in soup.select('a[href*="convmc_v2-files"][href$=".pdf"]')
        if a.get("href")
    ]
    # fallback más genérico por si cambian estructura
    if not pdf_links:
        pdf_links = [
            urljoin(BASE_URL + "/", a["href"])
            for a in soup.select('a[href$=".pdf"]')
            if a.get("href")
        ]

    results: List[Dict] = []

    # ---------- 4) Procesar cada bloque de convocatoria ----------
    for idx_pos, start in enumerate(indices):
        end = indices[idx_pos + 1] if idx_pos + 1 < len(indices) else len(cleaned_lines)
        chunk = cleaned_lines[start:end]

        if not chunk:
            continue

        solicitud_line = chunk[0]
        # numero: 4017-2025
        m_num = re.search(r"N[°º]\s*([0-9\-]+)", solicitud_line, flags=re.IGNORECASE)
        numero = m_num.group(1).strip() if m_num else ""

        numero_convocatoria = solicitud_line.strip()

        descripcion = ""
        publicado_el = ""
        tipo = ""
        fecha_limite = ""
        hora_limite = ""
        estado = ""

        for l in chunk[1:]:
            up = l.upper()

            # Publicado el XX/XX/XXXX
            if "PUBLICADO EL" in up and not publicado_el:
                m_pub = re.search(r"(\d{2}/\d{2}/\d{4})", l)
                if m_pub:
                    publicado_el = m_pub.group(1)
                continue

            # Tipo: BIENES / SERVICIO
            if up in ("BIENES", "SERVICIO"):
                if not tipo:
                    tipo = up
                continue

            # Fecha límite
            if not fecha_limite:
                m_f = re.search(r"(\d{2}/\d{2}/\d{4})", l)
                if m_f:
                    fecha_limite = m_f.group(1)
                    continue

            # Hora límite
            if not hora_limite:
                m_h = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", up)
                if m_h:
                    hora_limite = m_h.group(1).upper()
                    continue

            # Estado (VIGENTE / VENCIDO) — corregido para tomar SOLO el texto
            # dentro de paréntesis, ignorando lo que viene después.
            if not estado and "(" in up and ")" in up:
                m_state = re.search(r"\(([^()]+)\)", up)
                if m_state:
                    estado = m_state.group(1).strip().upper()
                continue

            # Descripción (primer texto que no sea cabecera ni "Publicado el")
            if not descripcion:
                if "PUBLICADO EL" in up:
                    continue
                if "DESCRIPCION" in up or "DESCRIPCIÓN" in up:
                    continue
                if "TIPO PLAZO DESCARGAR" in up:
                    continue
                descripcion = l.strip()

        # Solo convocatorias VIGENTES
        if estado != "VIGENTE":
            continue

        item: Dict[str, Optional[str]] = {
            "numero_convocatoria": numero_convocatoria,
            "numero": numero,
            "descripcion": descripcion,
            "publicado_el": publicado_el,
            "tipo": tipo,
            "fecha_limite": fecha_limite,
            "hora_limite": hora_limite,
            "estado": estado,
            "pagina_origen": page_number,
            "tdr_url": None,
            "tdr_filename": None,
            "tdr_downloaded": False,
            "caracteristicas_tecnicas": None,
            "caracteristicas_tecnicas_ocr": False,
        }

        results.append(item)

    # ---------- 5) Asignar PDFs a las convocatorias en orden ----------
    for i, item in enumerate(results):
        if i < len(pdf_links):
            item["tdr_url"] = pdf_links[i]

            pdf_path = download_pdf(session, item["tdr_url"], PDF_DIR)
            if pdf_path:
                item["tdr_filename"] = pdf_path.name
                item["tdr_downloaded"] = True
                block, used_ocr = extract_caracteristicas_from_pdf(str(pdf_path), True)
                item["caracteristicas_tecnicas"] = block
                item["caracteristicas_tecnicas_ocr"] = bool(used_ocr)

    logging.info(
        "Página %s: convocatorias VIGENTES encontradas: %s",
        page_number,
        len(results),
    )
    return results


# ============================================================
# BLOQUE 4: Scraping multi-página, orden y guardado JSON
# ============================================================

def sort_convocatorias(convocatorias: List[Dict]) -> List[Dict]:
    """
    Ordena las convocatorias por fecha_limite + hora_limite (más próximas primero).
    """
    def sort_key(item: Dict):
        fecha = item.get("fecha_limite") or ""
        hora = item.get("hora_limite") or "00:00 AM"
        try:
            dt = datetime.strptime(f"{fecha} {hora}", "%d/%m/%Y %I:%M %p")
        except ValueError:
            dt = datetime.min
        return dt

    return sorted(convocatorias, key=sort_key)


def scrape_convocatorias_vigentes() -> List[Dict]:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        }
    )

    all_items: List[Dict] = []

    for page in range(START_PAGE, START_PAGE + MAX_PAGES):
        url = build_page_url(page)
        html = get_html(session, url)
        if html is None:
            # 404 o error grave → asumimos fin
            break

        page_items = parse_page_convocatorias(session, html, page)
        if not page_items and page > START_PAGE:
            # Página sin convocatorias después de haber encontrado en anteriores → fin
            break

        all_items.extend(page_items)

    return sort_convocatorias(all_items)


def save_to_json(convocatorias: List[Dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "metadata": {
            "source": BASE_URL,
            "scraped_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "total_convocatorias_vigentes": len(convocatorias),
        },
        "convocatorias": convocatorias,
    }

    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logging.info("JSON generado: %s (total=%s)", path, len(convocatorias))


def main() -> None:
    logging.info("Iniciando scraper IMA 8UIT...")
    convocatorias = scrape_convocatorias_vigentes()
    save_to_json(convocatorias, OUTPUT_JSON)
    logging.info("Scraper IMA 8UIT finalizado.")


if __name__ == "__main__":
    main()
