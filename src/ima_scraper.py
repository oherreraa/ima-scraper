#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IMA 8UIT Scraper

Objetivo:
- Recorrer las páginas:
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---1.html
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---2.html
    ...
  hasta que ya no existan convocatorias.

- De cada convocatoria:
    · numero_convocatoria (ej. 'SOLICITUD DE COTIZACION N° 4017-2025')
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
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from PIL import Image
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

    # Cortamos muy largos para evitar JSON monstruoso
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
        logging.warning(f"No se pudo abrir PDF '{pdf_path}': {e}")
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

def parse_fecha_hora_estado(text: str) -> Tuple[str, str, str]:
    """
    A partir del texto completo del "card" de la convocatoria
    extrae:
      - fecha_limite (dd/mm/yyyy)
      - hora_limite (hh:mm AM/PM)
      - estado (entre paréntesis, ej. VIGENTE / VENCIDO)
    """
    plazo_clean = " ".join(text.split())

    m_estado = re.search(r"\(([^()]*)\)\s*$", plazo_clean)
    estado = m_estado.group(1).strip().upper() if m_estado else ""

    m_fecha = re.search(r"(\d{2}/\d{2}/\d{4})", plazo_clean)
    fecha = m_fecha.group(1) if m_fecha else ""

    m_hora = re.search(
        r"(\d{1,2}:\d{2}\s*[AP]M)", plazo_clean, flags=re.IGNORECASE
    )
    hora = m_hora.group(1).upper() if m_hora else ""

    return fecha, hora, estado


def parse_descripcion_block(desc_text: str) -> Tuple[str, str, str]:
    """
    A partir del texto de la "card" que contiene:

      'SOLICITUD DE COTIZACION N° 4017-2025
       SERVICIO DE ...
       | Publicado el 10/12/2025 | ...'

    Devuelve:
      - numero -> '4017-2025'
      - descripcion -> 'SERVICIO DE ...'
      - publicado_el -> '10/12/2025'
    """
    txt = " ".join(desc_text.split())

    m_num = re.search(
        r"SOLICITUD\s+DE\s+COTIZACION\s*N[°º]\s*([0-9\-]+)",
        txt,
        flags=re.IGNORECASE,
    )
    numero = m_num.group(1).strip() if m_num else ""

    m_pub = re.search(
        r"PUBLICADO\s+EL\s+([0-9]{2}/[0-9]{2}/[0-9]{4})",
        txt,
        flags=re.IGNORECASE,
    )
    publicado_el = m_pub.group(1) if m_pub else ""

    desc_region = txt
    if m_num:
        desc_region = desc_region[m_num.end():].strip()
    if m_pub:
        desc_region = desc_region[:m_pub.start()].strip()

    desc_region = desc_region.replace("|", " ").strip()
    descripcion = " ".join(desc_region.split())

    return numero, descripcion, publicado_el


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
    Parsea una página HTML del IMA y devuelve las convocatorias VIGENTES
    ya enriquecidas con información del PDF (si existe).
    """
    soup = BeautifulSoup(html, "html.parser")

    # Buscar nodos que contengan "SOLICITUD DE COTIZACION N°"
    pattern = re.compile(r"SOLICITUD\s+DE\s+COTIZACION\s*N[°º]\s*\d", re.IGNORECASE)
    text_nodes = soup.find_all(string=pattern)

    results: List[Dict] = []
    seen_containers = set()

    for node in text_nodes:
        container = node.parent

        # Subimos algunos niveles hasta encontrar un contenedor razonable
        # que tenga "Publicado el" y (idealmente) el link al PDF.
        for _ in range(6):
            if container is None:
                break
            text = container.get_text(" ", strip=True)
            if "Publicado el" in text:
                break
            container = container.parent

        if container is None:
            continue

        # Evitar usar el mismo contenedor varias veces
        key = id(container)
        if key in seen_containers:
            continue
        seen_containers.add(key)

        full_text = container.get_text(" ", strip=True)
        numero, descripcion, publicado_el = parse_descripcion_block(full_text)
        fecha_limite, hora_limite, estado = parse_fecha_hora_estado(full_text)

        # Solo convocatorias VIGENTES
        if estado != "VIGENTE":
            continue

        m_tipo = re.search(r"\b(BIENES|SERVICIO)\b", full_text, flags=re.IGNORECASE)
        tipo = m_tipo.group(1).upper() if m_tipo else ""

        # Link al PDF dentro del contenedor
        pdf_url = None
        link = container.find("a", href=re.compile(r"\.pdf\b", re.IGNORECASE))
        if link and link.get("href"):
            pdf_url = urljoin(BASE_URL + "/", link["href"])

        item: Dict[str, Optional[str]] = {
            "numero_convocatoria": (
                f"SOLICITUD DE COTIZACION N° {numero}" if numero else ""
            ),
            "numero": numero,
            "descripcion": descripcion,
            "publicado_el": publicado_el,
            "tipo": tipo,
            "fecha_limite": fecha_limite,
            "hora_limite": hora_limite,
            "estado": estado,
            "pagina_origen": page_number,
            "tdr_url": pdf_url,
            "tdr_filename": None,
            "tdr_downloaded": False,
            "caracteristicas_tecnicas": None,
            "caracteristicas_tecnicas_ocr": False,
        }

        # Descargar PDF y extraer CARACTERISTICAS TECNICAS
        if pdf_url:
            pdf_path = download_pdf(session, pdf_url, PDF_DIR)
            if pdf_path:
                item["tdr_filename"] = pdf_path.name
                item["tdr_downloaded"] = True
                block, used_ocr = extract_caracteristicas_from_pdf(str(pdf_path), True)
                item["caracteristicas_tecnicas"] = block
                item["caracteristicas_tecnicas_ocr"] = bool(used_ocr)

        results.append(item)

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
