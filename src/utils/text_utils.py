"""
Utilidades para normalización de texto
"""
import re
import unicodedata
from typing import Optional


def strip_accents(text: Optional[str]) -> str:
    """
    Eliminar acentos de una cadena de texto
    
    Args:
        text: Texto a procesar (puede ser None)
        
    Returns:
        Texto sin acentos
    """
    if text is None:
        return ""
    return "".join(
        c for c in unicodedata.normalize("NFD", str(text))
        if unicodedata.category(c) != "Mn"
    )


def normalize_text(text: Optional[str]) -> str:
    """
    Normalizar texto: eliminar acentos, convertir a minúsculas, normalizar espacios
    
    Args:
        text: Texto a normalizar
        
    Returns:
        Texto normalizado (sin acentos, minúsculas, espacios normalizados, guiones bajos)
    """
    if text is None:
        return ""
    
    # Eliminar acentos
    text = strip_accents(text)
    
    # Convertir a minúsculas y eliminar espacios al inicio/final
    text = text.strip().lower()
    
    # Normalizar espacios múltiples a uno solo
    text = re.sub(r"\s+", " ", text)
    
    # Reemplazar espacios con guiones bajos
    text = text.replace(" ", "_")
    
    return text

