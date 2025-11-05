# src/ine_api.py
import pandas as pd
import requests
from pathlib import Path

# -------------------------------------------------------
# CONFIGURACIÓN DE RUTAS Y DIRECTORIOS
# -------------------------------------------------------
DATA_RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
DATA_RAW.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------
# FUNCIONES BASE (descarga y carga)
# -------------------------------------------------------
def download_ine_xlsx(url: str, filename: str) -> Path:
    """
    Descarga un Excel del INE (formato JAXI-T3) y lo guarda en data/raw.
    Si el archivo ya existe, no lo vuelve a descargar.
    """
    path = DATA_RAW / filename
    if path.exists():
        print(f"✅ Archivo ya existe: {filename}")
        return path

    print(f"⏬ Descargando {filename} desde {url} ...")
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"❌ Error al descargar {url}: {response.status_code}")
    with open(path, "wb") as f:
        f.write(response.content)
    print(f"✅ Guardado: {path}")
    return path


def load_ine_xlsx(path: Path, header: int = 1) -> pd.DataFrame:
    """
    Carga el Excel del INE en un DataFrame.
    - header: número de fila que contiene los nombres de las columnas (0 o 1 según el caso)
    """
    df = pd.read_excel(path, sheet_name=0, header=header)
    df.dropna(how="all", inplace=True)
    df.columns = [str(col).strip().lower().replace(" ", "_") for col in df.columns]
    return df


# -------------------------------------------------------
# FUNCIONES ESPECÍFICAS DE INDICADORES INE
# -------------------------------------------------------

# 1️⃣ Renta media por distrito (ADRH – tabla 31097)
URL_RENTA = "https://www.ine.es/jaxiT3/files/t/xlsx/31097.xlsx"

def load_renta_media():
    path = download_ine_xlsx(URL_RENTA, "ine_renta_31097.xlsx")
    return load_ine_xlsx(path, header=1)

# 2️⃣ Indicadores demográficos por distrito (ADRH – tabla 31105)
URL_DEMOGRAFIA = "https://www.ine.es/jaxiT3/files/t/xlsx/31105.xlsx"

def load_indicadores_demograficos():
    path = download_ine_xlsx(URL_DEMOGRAFIA, "ine_demografia_31105.xlsx")
    return load_ine_xlsx(path, header=1)


# 3️⃣ Nivel educativo por sección censal (Censo – tabla 66753)
URL_EDUCACION = "https://www.ine.es/jaxiT3/files/t/xlsx/66753.xlsx"

def load_nivel_educativo():
    path = download_ine_xlsx(URL_EDUCACION, "ine_educacion_66753.xlsx")
    return load_ine_xlsx(path, header=1)


# 4️⃣ Vivienda / Hogares (ejemplo: tamaño medio del hogar está en 31105)
def load_tamano_hogar():
    df = load_indicadores_demograficos()
    mask = df.iloc[:, 0].astype(str).str.contains("hogar", case=False, na=False)
    return df[mask]


# 5️⃣ Personas mayores (65+) (también desde 31105)
def load_mayores_65():
    df = load_indicadores_demograficos()
    mask = df.iloc[:, 0].astype(str).str.contains("65", case=False, na=False)
    return df[mask]


# 6️⃣ Densidad de población (calcular a partir de población + superficie)
#    Aquí solo cargamos población; superficie la puedes traer del Ayuntamiento.
def load_poblacion():
    df = load_indicadores_demograficos()
    mask = df.iloc[:, 0].astype(str).str.contains("poblaci", case=False, na=False)
    return df[mask]


# -------------------------------------------------------
# MAIN DE PRUEBA
# -------------------------------------------------------
if __name__ == "__main__":
    print("\n📊 Renta media (31097):")
    df_renta = load_renta_media()
    print(df_renta.head())

    print("\n👥 Indicadores demográficos (31105):")
    df_demo = load_indicadores_demograficos()
    print(df_demo.head())

    print("\n📚 Nivel educativo (66753):")
    df_edu = load_nivel_educativo()
    print(df_edu.head())

    print("\n🏠 Tamaño medio del hogar (31105):")
    df_hogar = load_tamano_hogar()
    print(df_hogar.head())

    print("\n🧓 % Mayores de 65 años (31105):")
    df_mayores = load_mayores_65()
    print(df_mayores.head())

    print("\n🌍 Población (31105):")
    df_poblacion = load_poblacion()
    print(df_poblacion.head())
