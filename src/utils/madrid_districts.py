"""
Utilidades para manejo de distritos de Madrid
"""
from typing import Optional, Dict, List


# Mapeo oficial de número de distrito a nombre
MAD_DIST_NUM_TO_NAME: Dict[str, str] = {
    "01": "Centro",
    "02": "Arganzuela",
    "03": "Retiro",
    "04": "Salamanca",
    "05": "Chamartín",
    "06": "Tetuán",
    "07": "Chamberí",
    "08": "Fuencarral-El Pardo",
    "09": "Moncloa-Aravaca",
    "10": "Latina",
    "11": "Carabanchel",
    "12": "Usera",
    "13": "Puente de Vallecas",
    "14": "Moratalaz",
    "15": "Ciudad Lineal",
    "16": "Hortaleza",
    "17": "Villaverde",
    "18": "Villa de Vallecas",
    "19": "Vicálvaro",
    "20": "San Blas-Canillejas",
    "21": "Barajas",
}

# Mapeo inverso: nombre a número
MAD_DIST_NAME_TO_NUM: Dict[str, str] = {
    name: num for num, name in MAD_DIST_NUM_TO_NAME.items()
}

# Lista de todos los nombres de distritos
ALL_DISTRICTS: List[str] = list(MAD_DIST_NUM_TO_NAME.values())


def normalize_district_name(name: Optional[str]) -> Optional[str]:
    """
    Normalizar nombre de distrito a formato estándar oficial
    
    Args:
        name: Nombre del distrito (puede tener variaciones)
        
    Returns:
        Nombre oficial del distrito o None si no se encuentra
    """
    if name is None:
        return None
    
    # Normalizar entrada
    name_clean = name.strip()
    
    # Buscar coincidencia exacta
    if name_clean in MAD_DIST_NUM_TO_NAME.values():
        return name_clean
    
    # Buscar por número si viene como string numérico
    if name_clean in MAD_DIST_NUM_TO_NAME:
        return MAD_DIST_NUM_TO_NAME[name_clean]
    
    # Buscar coincidencia parcial (case-insensitive)
    name_lower = name_clean.lower()
    for official_name in MAD_DIST_NUM_TO_NAME.values():
        if name_lower == official_name.lower():
            return official_name
    
    # Buscar variaciones comunes
    variations = {
        "fuencarral-el pardo": "Fuencarral-El Pardo",
        "fuencarral el pardo": "Fuencarral-El Pardo",
        "moncloa-aravaca": "Moncloa-Aravaca",
        "moncloa aravaca": "Moncloa-Aravaca",
        "puente de vallecas": "Puente de Vallecas",
        "villa de vallecas": "Villa de Vallecas",
        "san blas-canillejas": "San Blas-Canillejas",
        "san blas canillejas": "San Blas-Canillejas",
    }
    
    if name_lower in variations:
        return variations[name_lower]
    
    return None


def get_district_number(name: Optional[str]) -> Optional[str]:
    """
    Obtener número de distrito (01-21) a partir del nombre
    
    Args:
        name: Nombre del distrito
        
    Returns:
        Número del distrito en formato "01"-"21" o None si no se encuentra
    """
    normalized = normalize_district_name(name)
    if normalized is None:
        return None
    return MAD_DIST_NAME_TO_NUM.get(normalized)


def get_district_name(number: Optional[str]) -> Optional[str]:
    """
    Obtener nombre oficial del distrito a partir del número
    
    Args:
        number: Número del distrito (01-21)
        
    Returns:
        Nombre oficial del distrito o None si no se encuentra
    """
    if number is None:
        return None
    
    # Asegurar formato de 2 dígitos
    num_str = str(number).zfill(2)
    return MAD_DIST_NUM_TO_NAME.get(num_str)


def get_all_districts() -> List[str]:
    """
    Obtener lista de todos los nombres oficiales de distritos
    
    Returns:
        Lista con los 21 nombres oficiales de distritos
    """
    return ALL_DISTRICTS.copy()


def is_valid_district(name: Optional[str]) -> bool:
    """
    Verificar si un nombre de distrito es válido
    
    Args:
        name: Nombre del distrito a verificar
        
    Returns:
        True si el distrito es válido, False en caso contrario
    """
    return normalize_district_name(name) is not None


def get_district_mapping() -> Dict[str, int]:
    """
    Obtener mapeo de nombre de distrito a número entero (1-21)
    Útil para integración con otros sistemas
    
    Returns:
        Diccionario con nombre -> número (1-21)
    """
    return {
        name: int(num) for num, name in MAD_DIST_NUM_TO_NAME.items()
    }

