# Módulo de Utilidades

Este módulo contiene funciones de utilidad compartidas por todo el proyecto.

## Módulos

### `text_utils.py`
Funciones para normalización de texto:
- `strip_accents()`: Eliminar acentos
- `normalize_text()`: Normalización completa de texto

### `madrid_districts.py`
Utilidades para manejo de distritos de Madrid:
- `MAD_DIST_NUM_TO_NAME`: Mapeo número → nombre
- `MAD_DIST_NAME_TO_NUM`: Mapeo nombre → número
- `normalize_district_name()`: Normalizar nombre de distrito
- `get_district_number()`: Obtener número de distrito
- `get_district_mapping()`: Mapeo para integración

### `file_utils.py`
Operaciones con archivos:
- `save_dataframe_to_csv()`: Guardar DataFrame en CSV
- `ensure_directory_exists()`: Asegurar que un directorio existe
- `get_data_directory()`: Obtener path de directorio de datos

## Uso

```python
from utils.text_utils import normalize_text
from utils.madrid_districts import normalize_district_name, get_district_number
from utils.file_utils import save_dataframe_to_csv

# Normalizar texto
normalized = normalize_text("Texto con acentos y espacios")

# Normalizar distrito
district = normalize_district_name("centro")  # → "Centro"
num = get_district_number("Centro")  # → "01"

# Guardar DataFrame
save_dataframe_to_csv(df, "archivo.csv")
```

