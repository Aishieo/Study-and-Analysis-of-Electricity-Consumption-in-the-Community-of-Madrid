"""
Módulo de utilidades compartidas para el proyecto TFM
"""

from .text_utils import strip_accents, normalize_text
from .madrid_districts import (
    MAD_DIST_NUM_TO_NAME,
    MAD_DIST_NAME_TO_NUM,
    normalize_district_name,
    get_district_number,
    get_all_districts
)
from .file_utils import save_dataframe_to_csv

__all__ = [
    'strip_accents',
    'normalize_text',
    'MAD_DIST_NUM_TO_NAME',
    'MAD_DIST_NAME_TO_NUM',
    'normalize_district_name',
    'get_district_number',
    'get_all_districts',
    'save_dataframe_to_csv',
]

