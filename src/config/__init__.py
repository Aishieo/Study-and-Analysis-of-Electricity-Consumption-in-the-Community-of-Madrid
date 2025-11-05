"""
Módulo de configuración centralizada para el proyecto TFM
"""

from .paths import BASE_DIR, DATA_RAW, DATA_PROCESSED
from .settings import get_api_key, get_setting

__all__ = [
    'BASE_DIR',
    'DATA_RAW',
    'DATA_PROCESSED',
    'get_api_key',
    'get_setting',
]

