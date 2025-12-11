#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scraper de convocatorias vigentes de:
https://www.ima.org.pe/adquisiciones-bienes-servicios-v2/s---1.html

Salida: data/convocatorias_vigentes.json
"""

# ============================================================
# BLOQUE 1: Imports y constantes
# ============================================================

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup
from requests import Response
from urllib.parse import urljoin

BASE_URL = "https://www.ima.org.pe/adquisiciones-bienes-servicios-v2"
START_PAGE = 1
MAX_PAGES = 30  # límite de seguridad, por si cambian el paginado

OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "convocatorias_vigentes.json"
)

# Configuración básica de logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


# ============================================================
# BLOQUE 2: Funciones auxiliares de red y parsing
# ============================================================

def build_page_url(page: int) -> str:
    """
    Construye la URL de página:
    s---1.html, s---2.html, etc., siempre colgando de BASE_URL.
    """
    return f"{BASE_URL}/s---{page}.html"


def get_html(session: requests.Session, url: str) -> Optional[str]:
    """
    Descarga HTML y maneja errores básicos.
    Devuelve el texto HTML o None si es 404 u otro error crítico.
    """
    try:
        logging.info(f"Descargando: {url}")
        resp: Response = session.get(url, timeout=30)
        if resp.status_code == 404:
            logging.warning(f"Página no encontrada (404): {url}")
            return None
        resp.raise_for_status()
        # Asegurar codificación correcta
        resp.encoding = resp.apparent_encoding
        return resp.text
    except requests.RequestException as exc:
        logging.error(f"Error al descargar {url}: {exc}")
        return None


def find_convocatorias_table(soup: BeautifulSoup):
    """
    Ubica la tabla que contiene:
    DESCRIPCION | TIPO | PLAZO | DESCARGAR
    """
    for table in soup.find_all("table"):
        text = table.get_text(" ", strip=True).upper()
        if "DESCRIPCION" in text and "PLAZO" in text:
            return table
    return None


def parse_fecha_hora_estado(plazo_text: str):
    """
    Ejemplo de texto en la columna PLAZO:
    '14/11/2024 10:30 AM (VENCIDO)'
    '12/12/2025 4:30 PM (VIGENTE)'
    """
    plazo_clean = " ".join(plazo_text.split())
    # Estado entre paréntesis
    m_estado = re.search(r"\(([^()]*)\)\s*$", plazo_clean)
    estado = m_estado.group(1).strip().upper() if m_estado else ""

    # Fecha dd/mm/yyyy
    m_fecha = re.search(r"(\d{2}/\d{2}/\d{4})", plazo_clean)
    fecha = m_fecha.group(1) if m_fecha else ""

    # Hora hh:mm AM/PM
    m_hora = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)", plazo_clean, flags=re.IGNORECASE)
    hora = m_hora.group(1).upper() if m_hora else ""

    return fecha, hora, estado


def parse_descripcion_block(desc_text: str):
    """
    desc_text típico (todo en una sola cadena):

    'SOLICITUD DE COTIZACION N° 3983-2025
     SERVICIO DE ACONDICIONAMIENTO DE COBERTURAS
     | Publicado el 28/11/2025 |'

    Se extrae:
      - numero
      - descripcion
      - publicado_el
    """
    # Normalizar espacios
    txt = " ".join(desc_text.split())
    # Número de solicitud
    m_num = re.search(
        r"SOLICITUD\s+DE\s+COTIZACION\s*N[°º]\s*([0-9\-]+)",
        txt,
        flags=re.IGNORECASE,
    )
    numero = m_num.group(1).strip() if m_num else ""

    # Fecha de publicación
    m_pub = re.search(
        r"PUBLICADO\s+EL\s+([0-9]{2}/[0-9]{2}/[0-9]{4})",
        txt,
        flags=re.IGNORECASE,
    )
    publicado_el = m_pub.group(1) if m_pub else ""

    # Para descripción, quitamos el prefijo de la solicitud y el bloque de publicación
    desc_region = txt
    if m_num:
        desc_region = desc_region[m_num.end():].strip()
    if m_pub:
        desc_region = desc_region[:m_pub.start()].strip()

    # Eliminar barras verticales si las hubiera
    desc_region = desc_region.replace("|", " ").strip()

    descripcion = " ".join(desc_region.split())
    return numero, descripcion, publicado_el


def parse_row(tr, page: int) -> Optional[Dict]:
    """
    Parsea una fila de la tabla de convocatorias.
    Devuelve un dict SOLO si el estado es VIGENTE.
    """
    cells = tr.find_all("td")
    if len(cells) < 3:
        return None

    # Columna DESCRIPCION (contiene "SOLICITUD DE COTIZACION N° ...", descripción, publicado)
    desc_text = " ".join(cells[0].stripped_strings)
    numero, descripcion, publicado_el = parse_descripcion_block(desc_text)

    # Columna TIPO (BIENES / SERVICIO)
    tipo_text = " ".join(cells[1].stripped_strings)
    tipo = tipo_text.strip().upper()

    # Columna PLAZO (fecha límite, hora, estado)
    plazo_text = " ".join(cells[2].stripped_strings)
    fecha_limite, hora_limite, estado = parse_fecha_hora_estado(plazo_text)

    # Solo nos interesan las convocatorias VIGENTES
    if estado != "VIGENTE":
        return None

    # Columna DESCARGAR (si existe)
    url_descarga = None
    if len(cells) >= 4:
        link = cells[3].find("a", href=True)
        if link:
            url_descarga = urljoin(BASE_URL + "/", link["href"])

    return {
        "numero": numero,
        "descripcion": descripcion,
        "publicado_el": publicado_el,      # dd/mm/yyyy
        "tipo": tipo,                      # BIENES / SERVICIO
        "fecha_limite": fecha_limite,      # dd/mm/yyyy
        "hora_limite": hora_limite,        # hh:mm AM/PM
        "estado": estado,                  # siempre 'VIGENTE' aquí
        "url_descarga": url_descarga,
        "pagina_origen": page,
    }


# ============================================================
# BLOQUE 3: Scraping multi-página y ordenado
# ============================================================

def scrape_convocatorias_vigentes() -> List[Dict]:
    """
    Recorre las páginas s---1.html, s---2.html, ... hasta:
      - encontrar 404, o
      - no hallar tabla de convocatorias

    Devuelve lista de dicts SOLO con convocatorias VIGENTES.
    """
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

    resultados: List[Dict] = []

    for page in range(START_PAGE, START_PAGE + MAX_PAGES):
        url = build_page_url(page)
        html = get_html(session, url)
        if html is None:
            # Probablemente 404 o error de red crítico
            logging.info(f"Fin del paginado en página {page}.")
            break

        soup = BeautifulSoup(html, "html.parser")
        table = find_convocatorias_table(soup)
        if not table:
            logging.info(f"Sin tabla de convocatorias en página {page}. Deteniendo.")
            break

        filas = table.find_all("tr")
        if len(filas) <= 1:
            logging.info(f"Tabla sin filas de datos en página {page}. Deteniendo.")
            break

        logging.info(f"Procesando página {page} con {len(filas) - 1} filas de datos.")
        for tr in filas[1:]:
            item = parse_row(tr, page)
            if item:
                resultados.append(item)

    # Ordenar por fecha_limite + hora_limite (más próximas primero)
    def sort_key(item: Dict):
        fecha = item.get("fecha_limite") or ""
        hora = item.get("hora_limite") or "00:00 AM"
        try:
            dt = datetime.strptime(f"{fecha} {hora}", "%d/%m/%Y %I:%M %p")
        except ValueError:
            dt = datetime.min
        return dt

    resultados.sort(key=sort_key)
    return resultados


# ============================================================
# BLOQUE 4: Escritura de JSON y punto de entrada
# ============================================================

def save_to_json(convocatorias: List[Dict], path: Path) -> None:
    """
    Escribe un único archivo JSON con:
      - metadata
      - lista de convocatorias vigentes
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "metadata": {
            "source": str(build_page_url(START_PAGE)),
            "base_url": BASE_URL,
            "scraped_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "total_convocatorias_vigentes": len(convocatorias),
        },
        "convocatorias": convocatorias,
    }

    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    logging.info(f"JSON generado: {path} ({len(convocatorias)} vigentes)")


def main():
    convocatorias = scrape_convocatorias_vigentes()
    save_to_json(convocatorias, OUTPUT_PATH)


if __name__ == "__main__":
    main()

