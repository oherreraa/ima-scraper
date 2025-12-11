#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
IMA 8UIT Scraper

Objetivo:
- Recorrer las páginas:
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s.html (página 1)
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---2.html
    https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---3.html
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
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time

import requests
from bs4 import BeautifulSoup
from PIL import Image  # noqa: F401 (no se usa directamente, pero útil para tipos)
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
        "ESPECIFICACIONES TECNICAS",
        "ESPECIFICACIONES TÉCNICAS",
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

def extract_numero_from_title(title: str) -> str:
    """Extrae el número de convocatoria del título."""
    m = re.search(r"N[°º]\s*([0-9\-]+)", title, flags=re.IGNORECASE)
    return m.group(1).strip() if m else ""


def extract_fecha_from_text(text: str) -> str:
    """Extrae fecha en formato dd/mm/yyyy del texto."""
    m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
    return m.group(1) if m else ""


def extract_publicado_el(text: str) -> str:
    """Extrae la fecha de publicación."""
    m = re.search(r"Publicado el (\d{2}/\d{2}/\d{4})", text, re.IGNORECASE)
    return m.group(1) if m else ""


def parse_plazo_cell(plazo_text: str) -> Tuple[str, str, str]:
    """
    Parsea el contenido de la celda PLAZO que contiene:
    - fecha límite (dd/mm/yyyy)
    - hora límite (hh:mm AM/PM)  
    - estado (VIGENTE/VENCIDO) entre paréntesis
    """
    plazo_clean = " ".join(plazo_text.split())
    
    # Estado entre paréntesis
    m_estado = re.search(r"\(([^()]*)\)\s*$", plazo_clean)
    estado = m_estado.group(1).strip().upper() if m_estado else ""

    # Fecha
    m_fecha = re.search(r"(\d{2}/\d{2}/\d{4})", plazo_clean)
    fecha = m_fecha.group(1) if m_fecha else ""

    # Hora
    m_hora = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", plazo_clean, flags=re.IGNORECASE)
    hora = m_hora.group(1).upper() if m_hora else ""

    return fecha, hora, estado


def build_page_url(page: int) -> str:
    """Construye la URL de la página. La primera página es especial."""
    if page == 1:
        return f"{BASE_URL}/s.html"
    else:
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
    Parsea una página HTML del IMA usando selectores CSS más robustos.
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Debug: guardar HTML para inspección
    debug_path = ROOT_DIR / "data" / f"debug_page_{page_number}.html"
    debug_path.parent.mkdir(parents=True, exist_ok=True)
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)
    logging.info("HTML guardado para debug: %s", debug_path)

    results: List[Dict] = []

    # Estrategia 1: Buscar tabla con estructura típica
    # Buscar todas las filas que contengan "SOLICITUD DE COTIZACION"
    rows = soup.find_all("tr")
    
    convocatoria_rows = []
    for row in rows:
        row_text = row.get_text()
        if "SOLICITUD DE COTIZACION" in row_text.upper():
            convocatoria_rows.append(row)
    
    logging.info("Página %d: Encontradas %d filas con 'SOLICITUD DE COTIZACION'", 
                 page_number, len(convocatoria_rows))

    # Estrategia 2: Si no hay tabla, buscar por texto
    if not convocatoria_rows:
        full_text = soup.get_text()
        logging.info("Página %d: Contenido de texto (primeros 500 chars):\n%s", 
                     page_number, full_text[:500])
        
        # Buscar patrones en el texto
        solicitud_pattern = r"SOLICITUD DE COTIZACION N[°º]\s*([0-9\-]+)"
        matches = re.findall(solicitud_pattern, full_text, re.IGNORECASE)
        logging.info("Página %d: Encontrados %d patrones de solicitud: %s", 
                     page_number, len(matches), matches)

    # Buscar enlaces PDF
    pdf_links = []
    
    # Patrón 1: convmc-files (sin _v2)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "convmc-files" in href and href.endswith(".pdf"):
            full_url = urljoin(BASE_URL + "/", href)
            pdf_links.append(full_url)
    
    # Patrón 2: cualquier PDF
    if not pdf_links:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.endswith(".pdf"):
                full_url = urljoin(BASE_URL + "/", href)
                pdf_links.append(full_url)

    logging.info("Página %d: Encontrados %d enlaces PDF: %s", 
                 page_number, len(pdf_links), pdf_links)

    # Procesar cada fila de convocatoria encontrada
    for i, row in enumerate(convocatoria_rows):
        try:
            cells = row.find_all(["td", "th"])
            row_text = row.get_text()
            
            logging.info("Procesando fila %d: %s", i, row_text[:100])
            
            # Extraer información básica
            numero_convocatoria = ""
            numero = ""
            descripcion = ""
            publicado_el = ""
            tipo = ""
            fecha_limite = ""
            hora_limite = ""
            estado = ""
            
            # Buscar en el texto de la fila
            lines = [line.strip() for line in row_text.split('\n') if line.strip()]
            
            for line in lines:
                line_upper = line.upper()
                
                # Número de convocatoria
                if "SOLICITUD DE COTIZACION" in line_upper and not numero_convocatoria:
                    numero_convocatoria = line.strip()
                    numero = extract_numero_from_title(line)
                
                # Publicado el
                elif "PUBLICADO EL" in line_upper and not publicado_el:
                    publicado_el = extract_publicado_el(line)
                
                # Tipo
                elif line_upper in ["BIENES", "SERVICIO"] and not tipo:
                    tipo = line_upper
                
                # Fecha y estado en misma línea
                elif re.search(r"\d{2}/\d{2}/\d{4}", line) and not fecha_limite:
                    fecha_limite, hora_limite, estado = parse_plazo_cell(line)
                
                # Descripción (línea que no coincide con patrones anteriores)
                elif (not descripcion and 
                      "SOLICITUD DE COTIZACION" not in line_upper and
                      "PUBLICADO EL" not in line_upper and
                      line_upper not in ["BIENES", "SERVICIO"] and
                      not re.search(r"\d{2}/\d{2}/\d{4}", line)):
                    descripcion = line.strip()

            # Solo guardar si tiene estado VIGENTE
            if estado != "VIGENTE":
                logging.info("Saltando convocatoria con estado '%s': %s", estado, numero_convocatoria)
                continue

            item = {
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

            # Asignar PDF si existe
            if i < len(pdf_links):
                item["tdr_url"] = pdf_links[i]
                
                # Descargar y procesar PDF
                pdf_path = download_pdf(session, item["tdr_url"], PDF_DIR)
                if pdf_path:
                    item["tdr_filename"] = pdf_path.name
                    item["tdr_downloaded"] = True
                    
                    # Extraer características técnicas
                    block, used_ocr = extract_caracteristicas_from_pdf(str(pdf_path), True)
                    item["caracteristicas_tecnicas"] = block
                    item["caracteristicas_tecnicas_ocr"] = bool(used_ocr)

            results.append(item)
            logging.info("Convocatoria procesada: %s", numero_convocatoria)

        except Exception as e:
            logging.error("Error procesando fila %d: %s", i, e)
            continue

    logging.info("Página %d: %d convocatorias VIGENTES procesadas", page_number, len(results))
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
            logging.info("No se pudo obtener página %d, terminando", page)
            break

        page_items = parse_page_convocatorias(session, html, page)
        
        if not page_items and page > START_PAGE:
            logging.info("Página %d sin convocatorias, terminando scraping", page)
            break

        all_items.extend(page_items)
        
        # Delay entre páginas
        time.sleep(1)

    logging.info("Total de convocatorias vigentes encontradas: %d", len(all_items))
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
