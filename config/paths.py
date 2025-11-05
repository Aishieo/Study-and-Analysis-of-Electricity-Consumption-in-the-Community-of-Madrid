"""
Configuración centralizada de rutas del proyecto
"""
from pathlib import Path

# Directorio base del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent

# Directorios de datos
DATA_PROCESSED = BASE_DIR / "data" / "processed"

# Asegurar que los directorios existen
DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
