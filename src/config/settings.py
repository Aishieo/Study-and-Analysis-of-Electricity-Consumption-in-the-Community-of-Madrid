"""
Configuración centralizada del proyecto
Maneja variables de entorno y configuraciones
"""
import os
from typing import Optional
from pathlib import Path

# Directorio base (para referencias relativas)
BASE_DIR = Path(__file__).resolve().parent.parent


def get_api_key(service: str, default: Optional[str] = None) -> str:
    """
    Obtener API key desde variable de entorno
    
    Args:
        service: Nombre del servicio (ej: 'ESIOS', 'OPENWEATHER')
        default: Valor por defecto si no se encuentra (opcional)
        
    Returns:
        API key del servicio
        
    Raises:
        ValueError: Si no se encuentra la key y no hay default
    """
    env_var = f"{service}_API_KEY"
    api_key = os.getenv(env_var, default)
    
    if api_key is None:
        raise ValueError(
            f"{env_var} no está configurada. "
            f"Por favor, configúrala como variable de entorno o en un archivo .env"
        )
    
    return api_key


def get_setting(key: str, default: Optional[str] = None) -> str:
    """
    Obtener configuración desde variable de entorno
    
    Args:
        key: Nombre de la configuración
        default: Valor por defecto si no se encuentra
        
    Returns:
        Valor de la configuración
    """
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"Configuración '{key}' no encontrada y no hay valor por defecto")
    return value


# URLs de APIs públicas
INE_BASE_URL = "https://www.ine.es"
INE_XLSX_BASE_URL = f"{INE_BASE_URL}/jaxi/files/tpx/xlsx"
INE_59532_URL = f"{INE_XLSX_BASE_URL}/59532.xlsx"

# Nombres de archivos estándar
FNAME_INE_59532 = "ine_consumo_electrico_59532.xlsx"

