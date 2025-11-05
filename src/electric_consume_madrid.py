# Crear un Excel nuevo con SOLO los 21 distritos de Madrid (a partir del 59532 ya subido),
# normalizando percentiles y añadiendo nº/ nombre de distrito.
#
# Salida: data/raw/madrid_distritos_consumo_59532.xlsx

import re
from pathlib import Path
import pandas as pd
import numpy as np

# Importar utilidades centralizadas
from config.paths import DATA_RAW
from config.settings import FNAME_INE_59532
from utils.text_utils import normalize_text
from utils.madrid_districts import MAD_DIST_NUM_TO_NAME

PATH_INE_59532 = DATA_RAW / FNAME_INE_59532
OUT_XLSX = DATA_RAW / "madrid_distritos_consumo_59532.xlsx"

# Función local para normalización (usa utilidad centralizada)
def _norm(s: str) -> str:
    """Normalizar texto usando utilidad centralizada"""
    return normalize_text(s)

def _standardize_percentile_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {}
    for c in df.columns:
        cn = _norm(c)
        m = re.search(r"percentil[_ ]?([0-9]{2})", cn)
        if m:
            p = m.group(1)
            rename[c] = f"p{p}"
    if rename:
        df = df.rename(columns=rename)
    return df

def _extract_municipio_code(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    m = re.match(r"\s*(\d{5})", text)
    return m.group(1) if m else None

def _extract_distrito_num_madrid(text: str) -> str | None:
    if not isinstance(text, str):
        return None
    if not re.match(r"\s*28079", text):  # Madrid ciudad
        return None
    m = re.search(r"distrito\D*([0-2]?\d)\b", text, flags=re.IGNORECASE)
    if not m:
        return None
    return m.group(1).zfill(2)

# --- Carga y preparación ---
# El fichero que subiste trae cabecera en fila 7 (1-based) -> header=6
df = pd.read_excel(PATH_INE_59532, sheet_name=0, header=6, engine="openpyxl")
df.dropna(how="all", inplace=True)
df.columns = [str(c).strip() for c in df.columns]

# Asegurar que la primera columna es "Distritos"
first_col = df.columns[0]
if _norm(first_col) != "distritos":
    df = df.rename(columns={first_col: "Distritos"})
col_dist = "Distritos"

# Filtrar Madrid (municipio 28079) y quitar totales
muni = df[col_dist].astype(str).apply(_extract_municipio_code)
mask_mad = (muni == "28079")
out = df.loc[mask_mad].copy()
out = out.loc[~out[col_dist].astype(str).str.contains(r"\btotal\b", case=False, na=False)].copy()

# Estandarizar percentiles y convertir a numéricos
out = _standardize_percentile_columns(out)
for p in ["p10", "p25", "p50", "p75", "p90"]:
    if p in out.columns:
        out[p] = pd.to_numeric(out[p], errors="coerce")

# Añadir nº y nombre de distrito y reordenar columnas
out["distrito_num"] = out[col_dist].astype(str).apply(_extract_distrito_num_madrid)
out["distrito_nombre"] = out["distrito_num"].map(MAD_DIST_NUM_TO_NAME)

# Reordenación amigable
cols_front = [col_dist, "distrito_num", "distrito_nombre"]
perc = [c for c in ["p10", "p25", "p50", "p75", "p90"] if c in out.columns]
other = [c for c in out.columns if c not in cols_front + perc]
out = out[cols_front + perc + other].reset_index(drop=True)

# Asegurar 21 distritos (si hay menos, dejar aviso en log)
distritos_detectados = out["distrito_num"].dropna().unique().tolist()
ok_21 = len(distritos_detectados) == 21

# Guardado a XLSX con una hoja de diccionario + log
with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as writer:
    out.to_excel(writer, index=False, sheet_name="distritos_consumo")
    # Diccionario
    dict_rows = []
    for c in out.columns:
        if c in ["Distritos", "distrito_num", "distrito_nombre"]:
            desc = "Identificador territorial y etiquetas de distrito (Madrid ciudad)."
        elif c in ["p10","p25","p50","p75","p90"]:
            desc = f"Percentil {c[1:]} del consumo eléctrico (kWh)."
        else:
            desc = "Columna original del INE 59532 (revisar etiqueta para detalle)."
        dict_rows.append({"columna": c, "descripcion": desc})
    pd.DataFrame(dict_rows).to_excel(writer, index=False, sheet_name="diccionario")

print("Archivo generado:", str(OUT_XLSX))
print("Total filas:", len(out))
