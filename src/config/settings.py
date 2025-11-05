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
    
    # Temporal: Para mantener compatibilidad durante transición
    # TODO: Eliminar este fallback después de migración completa
    if api_key is None and service == "ESIOS":
        # Fallback temporal - AVISO: Esto debe eliminarse
        legacy_key = "79ebcce6833ab7973be74132360881e1c856b1823f5047225cb06fe056bc4a7b"
        import warnings
        warnings.warn(
            f"⚠️ {env_var} no está configurada. Usando key temporal. "
            f"Por favor, configura {env_var} como variable de entorno.",
            UserWarning
        )
        return legacy_key
    
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

