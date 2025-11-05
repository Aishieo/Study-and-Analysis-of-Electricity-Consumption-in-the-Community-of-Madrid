# Módulo de Configuración

Este módulo contiene la configuración centralizada del proyecto.

## Archivos

- `paths.py`: Rutas de directorios del proyecto
- `settings.py`: Configuración de APIs y variables de entorno
- `logging_config.py`: Configuración de logging

## Uso

```python
from config.paths import DATA_RAW, DATA_PROCESSED
from config.settings import get_api_key

# Obtener API key
api_key = get_api_key("ESIOS")

# Usar rutas
file_path = DATA_RAW / "archivo.csv"
```

