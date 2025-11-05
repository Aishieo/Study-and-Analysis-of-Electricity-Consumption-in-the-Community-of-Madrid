"""
Configuración centralizada de logging
"""
import logging
import sys
from pathlib import Path
from typing import Optional

# Directorio base
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def setup_logging(
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> None:
    """
    Configurar logging para todo el proyecto
    
    Args:
        level: Nivel de logging (default: INFO)
        log_file: Nombre del archivo de log (opcional)
        format_string: Formato personalizado (opcional)
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    handlers = [logging.StreamHandler(sys.stdout)]
    
    if log_file:
        log_path = LOG_DIR / log_file
        handlers.append(logging.FileHandler(log_path, encoding='utf-8'))
    
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=handlers,
        force=True  # Sobrescribir configuración anterior
    )


def get_logger(name: str) -> logging.Logger:
    """
    Obtener logger con configuración estándar
    
    Args:
        name: Nombre del logger (típicamente __name__ del módulo)
        
    Returns:
        Logger configurado
    """
    return logging.getLogger(name)


# Configurar logging por defecto al importar
setup_logging()

