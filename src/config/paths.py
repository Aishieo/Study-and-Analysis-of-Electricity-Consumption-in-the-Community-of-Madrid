"""
Configuración centralizada de rutas del proyecto
"""
from pathlib import Path

# Directorio base del proyecto (un nivel arriba de src/)
BASE_DIR = Path(__file__).resolve().parent.parent

# Directorios de datos
DATA_RAW = BASE_DIR / "data" / "raw"
DATA_PROCESSED = BASE_DIR / "data" / "processed"

# Directorios específicos
DATA_ESIOS = DATA_RAW / "esios"
DATA_ESIOS_HISTORICAL = DATA_ESIOS / "historical"
DATA_ESIOS_PROCESSED = DATA_PROCESSED / "esios"
DATA_ESIOS_PROCESSED_HISTORICAL = DATA_ESIOS_PROCESSED / "historical"

# Asegurar que los directorios existen
DATA_RAW.mkdir(parents=True, exist_ok=True)
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
DATA_ESIOS.mkdir(parents=True, exist_ok=True)
DATA_ESIOS_HISTORICAL.mkdir(parents=True, exist_ok=True)
DATA_ESIOS_PROCESSED.mkdir(parents=True, exist_ok=True)
DATA_ESIOS_PROCESSED_HISTORICAL.mkdir(parents=True, exist_ok=True)

