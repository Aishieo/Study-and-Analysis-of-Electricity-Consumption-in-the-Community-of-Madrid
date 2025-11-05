"""
Utilidades para operaciones con archivos
"""
import os
import logging
from pathlib import Path
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)


def save_dataframe_to_csv(
    df: pd.DataFrame,
    filename: str,
    subdirectory: str = "processed",
    base_dir: Optional[Path] = None
) -> Path:
    """
    Guardar DataFrame en formato CSV de forma estándar
    
    Args:
        df: DataFrame a guardar
        filename: Nombre del archivo (sin ruta)
        subdirectory: Subdirectorio dentro de data/ (default: "processed")
        base_dir: Directorio base del proyecto (opcional)
        
    Returns:
        Path del archivo guardado
        
    Raises:
        ValueError: Si el DataFrame está vacío
    """
    if df.empty:
        logger.warning(f"No hay datos para guardar en {filename}")
        raise ValueError(f"No hay datos para guardar en {filename}")
    
    # Determinar directorio base
    if base_dir is None:
        # Asumir que estamos en src/ y el proyecto está un nivel arriba
        base_dir = Path(__file__).resolve().parent.parent
    
    # Construir ruta completa
    filepath = base_dir / "data" / subdirectory / filename
    
    # Crear directorio si no existe
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    # Guardar CSV
    df.to_csv(filepath, index=False, encoding='utf-8')
    logger.info(f"Datos guardados en {filepath}")
    
    return filepath


def ensure_directory_exists(directory: Path) -> Path:
    """
    Asegurar que un directorio existe, creándolo si es necesario
    
    Args:
        directory: Path del directorio
        
    Returns:
        Path del directorio (ahora existente)
    """
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def get_data_directory(subdirectory: str = "processed", base_dir: Optional[Path] = None) -> Path:
    """
    Obtener path del directorio de datos
    
    Args:
        subdirectory: Subdirectorio dentro de data/ (default: "processed")
        base_dir: Directorio base del proyecto (opcional)
        
    Returns:
        Path del directorio de datos
    """
    if base_dir is None:
        base_dir = Path(__file__).resolve().parent.parent
    
    data_dir = base_dir / "data" / subdirectory
    ensure_directory_exists(data_dir)
    return data_dir

