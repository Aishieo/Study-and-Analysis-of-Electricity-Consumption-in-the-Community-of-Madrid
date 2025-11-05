# src/ine_api_electric.py
# -*- coding: utf-8 -*-
import re
from pathlib import Path

import pandas as pd
import requests

# Importar utilidades centralizadas
from config.paths import DATA_RAW
from config.settings import INE_59532_URL, FNAME_INE_59532
from utils.text_utils import normalize_text
from utils.madrid_districts import MAD_DIST_NUM_TO_NAME

# ----------------------------- Utilidades locales -----------------------------
def _norm(s: str) -> str:
    """Normalizar texto usando utilidad centralizada"""
    return normalize_text(s)

def _standardize_percentile_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas de percentiles a: p10, p25, p50, p75, p90.
    Acepta variantes como:
    - 'Percentil 10 de consumo eléctrico en kwh'
    - 'percentil_25', 'Percentil 50', etc.
    """
    rename = {}
    for c in df.columns:
        cn = _norm(c)
        m = re.search(r"percentil[_ ]?([0-9]{2})", cn)
        if m:
            p = m.group(1)  # '10'/'25'/'50'/'75'/'90'
            rename[c] = f"p{p}"
    if rename:
        df = df.rename(columns=rename)
    return df

def _extract_distrito_num(text: str) -> str | None:
    """
    Extrae '01'..'21' de cadenas tipo '... Madrid distrito 01 ...'
    """
    if not isinstance(text, str):
        return None
    m = re.search(r"distrito\D*([0-2]?\d)", text, flags=re.IGNORECASE)
    if not m:
        return None
    num = m.group(1).zfill(2)
    if num in MAD_DIST_NUM_TO_NAME:
        return num
    return None

# ----------------------------- Descarga / Carga -----------------------------
def download_ine_xlsx_59532() -> Path:
    """
    Descarga el Excel 59532 del INE (si no existe) y devuelve la ruta.
    """
    path = DATA_RAW / FNAME_INE_59532
    if path.exists():
        print(f"✅ Ya existe: {path.name}")
        return path

    print(f"⏬ Descargando {FNAME_INE_59532} desde {INE_59532_URL} ...")
    r = requests.get(INE_59532_URL, timeout=90)
    if r.status_code != 200:
        raise Exception(f"❌ Error al descargar {INE_59532_URL}: {r.status_code}")
    path.write_bytes(r.content)
    print(f"✅ Guardado: {path}")
    return path

def load_consumo_energia_59532(header_row: int = 6) -> pd.DataFrame:
    """
    Lee el Excel 59532. Por tu comentario, los encabezados están en la fila 7 (1-based),
    así que header_row=6 (0-based) por defecto.
    Asumo que la PRIMERA columna la has renombrado a 'Distritos'.
    """
    path = download_ine_xlsx_59532()
    df = pd.read_excel(path, sheet_name=0, header=header_row, engine="openpyxl")
    df.dropna(how="all", inplace=True)
    # normaliza nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]
    # Asegura que existe 'Distritos'
    first_col = df.columns[0]
    if _norm(first_col) != "distritos":
        # si aún no lo renombraste, lo hago aquí por si acaso
        df = df.rename(columns={first_col: "Distritos"})
    # estandariza percentiles
    df = _standardize_percentile_columns(df)
    return df

# ----------------------------- Filtro Madrid -----------------------------
# --- NUEVO: extraer código de municipio (primeros 5 dígitos) y nº de distrito ---
def _extract_municipio_code(text: str) -> str | None:
    """
    Devuelve el código de municipio INE (5 dígitos) del comienzo de la cadena, p.ej. '28079'
    para '2807901 Madrid distrito 01'.
    """
    if not isinstance(text, str):
        return None
    m = re.match(r"\s*(\d{5})", text)
    return m.group(1) if m else None

def _extract_distrito_num_madrid(text: str) -> str | None:
    """
    Devuelve '01'..'21' SOLO si la fila es de la ciudad de Madrid (municipio 28079).
    """
    if not isinstance(text, str):
        return None
    # Debe empezar por 28079 (Madrid ciudad)
    if not re.match(r"\s*28079", text):
        return None
    m = re.search(r"distrito\D*([0-2]?\d)\b", text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).zfill(2)

# --- SUSTITUIR: esta versión filtra SOLO Madrid ciudad por código 28079 ---
def load_consumo_madrid_distritos(header_row: int = 6, add_distrito_nombre: bool = True) -> pd.DataFrame:
    """
    Devuelve SOLO filas de la ciudad de Madrid (municipio 28079):
      - Filtra por código de municipio (28079) de la primera columna ('Distritos').
      - Estandariza columnas de percentiles a p10..p90.
      - Convierte p10..p90 a numéricos.
      - (Opcional) añade 'distrito_num' y 'distrito_nombre' (solo si aplica).
    """
    df = load_consumo_energia_59532(header_row=header_row)

    # localizar columna 'Distritos' (ya renombrada por ti)
    col_dist = [c for c in df.columns if _norm(c) == "distritos"][0]

    # FILTRO ESTRICTO: municipio 28079 (Madrid capital)
    muni = df[col_dist].astype(str).apply(_extract_municipio_code)
    mask_mad = (muni == "28079")
    out = df.loc[mask_mad].copy()

    # quitar totales si los hubiera
    out = out.loc[~out[col_dist].astype(str).str.contains(r"\btotal\b", case=False, na=False)].copy()

    # estandarizar y convertir percentiles
    out = _standardize_percentile_columns(out)
    for p in ["p10", "p25", "p50", "p75", "p90"]:
        if p in out.columns:
            out[p] = pd.to_numeric(out[p], errors="coerce")

    # Añadir nº y nombre de distrito SOLO para Madrid ciudad
    if add_distrito_nombre:
        out["distrito_num"] = out[col_dist].astype(str).apply(_extract_distrito_num_madrid)
        out["distrito_nombre"] = out["distrito_num"].map(MAD_DIST_NUM_TO_NAME)
        # Reordenar columnas si existen los percentiles
        cols = [col_dist, "distrito_num", "distrito_nombre"]
        perc = [c for c in ["p10", "p25", "p50", "p75", "p90"] if c in out.columns]
        other = [c for c in out.columns if c not in cols + perc]
        out = out[cols + perc + other]

    return out.reset_index(drop=True)

# ----------------------------- Main de prueba -----------------------------
if __name__ == "__main__":
    pd.set_option("display.width", 200)
    pd.set_option("display.max_columns", 50)

    print("\n⚡ Consumo eléctrico — 59532 (solo Madrid, distritos):")
    df_mad = load_consumo_madrid_distritos(header_row=6, add_distrito_nombre=True)
    print(df_mad.head(12))

    # ejemplo de ranking por desigualdad (p90 - p10) si están presentes
    if all(c in df_mad.columns for c in ["p10", "p90"]):
        df_rank = df_mad.assign(desigualdad=df_mad["p90"] - df_mad["p10"]) \
                        .sort_values("desigualdad", ascending=False)
        print("\n🏁 Top distritos por desigualdad (p90 - p10):")
        print(df_rank[[df_mad.columns[0], "distrito_num", "distrito_nombre", "p10", "p90", "desigualdad"]].head(10))
