"""
Módulo para recopilar datos de movilidad y transporte público de Madrid por distritos
Utiliza datos del Consorcio Regional de Transportes y Ayuntamiento de Madrid
"""

import pandas as pd
import os
from typing import Dict
import logging

# Configurar logging usando configuración centralizada
from config.logging_config import get_logger
logger = get_logger(__name__)

class MobilityDataCollector:
    """
    Clase para recopilar datos de movilidad y transporte público de Madrid
    """
    
    def __init__(self):
        """
        Inicializar el colector de datos de movilidad
        """
        
        # Estaciones de metro y cercanías por distrito
        self.transport_stations = {
            # 1. CENTRO
            "Centro": {
                "metro_stations": [
                    "Sol", "Gran Vía", "Callao", "Santo Domingo",
                    "Noviciado", "Tribunal", "Chueca",
                    "Sevilla", "Banco de España",
                    "Ópera", "La Latina", "Tirso de Molina",
                    "Lavapiés", "Antón Martín"
                ],
                "cercanias_stations": [
                    "Sol"
                ],
                "bus_lines": [
                    3, 5, 9, 15, 20,
                    46, 50, 51, 52, 53,
                    150, 17, 18, 23, 31, 35
                ],
                "accessibility_score": 0.95
            },
            
            # 2. ARGANZUELA
            "Arganzuela": {
                "metro_stations": [
                    "Embajadores", "Acacias", "Pirámides",
                    "Legazpi", "Delicias",
                    "Palos de la Frontera", "Méndez Álvaro",
                    "Arganzuela-Planetario"
                ],
                "cercanias_stations": [
                    "Embajadores", "Delicias", "Méndez Álvaro"
                ],
                "bus_lines": [
                    6, 8, 18, 19, 45,
                    47, 55, 59, 85, 86
                ],
                "accessibility_score": 0.9
            },
            
            # 3. RETIRO
            "Retiro": {
                "metro_stations": [
                    "Retiro", "Ibiza", "Sainz de Baranda",
                    "O'Donnell", "Príncipe de Vergara",
                    "Menéndez Pelayo", "Pacífico",
                    "Conde de Casal"
                ],
                "cercanias_stations": [
                    "Atocha"
                ],
                "bus_lines": [
                    2, 14, 19, 20, 26,
                    30, 32, 56, 63
                ],
                "accessibility_score": 0.88
            },
            
            # 4. SALAMANCA
            "Salamanca": {
                "metro_stations": [
                    "Goya", "Lista", "Manuel Becerra",
                    "Ventura Rodríguez",  # límite oeste, sirve parte del distrito
                    "Príncipe de Vergara", "Velázquez",
                    "Serrano", "Núñez de Balboa", "Diego de León"
                ],
                "cercanias_stations": [
                    "Recoletos"
                ],
                "bus_lines": [
                    1, 2, 9, 12, 14, 19, 21, 26, 28,
                    43, 48, 53, 74, 143, 146, 152, 156
                ],
                "accessibility_score": 0.9
            },
            
            # 5. CHAMARTÍN
            "Chamartín": {
                "metro_stations": [
                    "Chamartín", "Plaza de Castilla",
                    "Duque de Pastrana", "Pío XII",
                    "Bambú", "Colombia", "Cuzco"
                ],
                "cercanias_stations": [
                    "Chamartín",
                    "Nuevos Ministerios"
                ],
                "bus_lines": [
                    5, 11, 14, 16, 27,
                    40, 147, 150,
                    129, 134, 135
                ],
                "accessibility_score": 0.95
            },
            
            # 6. TETUÁN
            "Tetuán": {
                "metro_stations": [
                    "Tetuán", "Valdeacederas",
                    "Cuzco", "Plaza de Castilla",
                    "Estrecho", "Alvarado"
                ],
                "cercanias_stations": [],
                "bus_lines": [
                    11, 44, 66, 124, 126,
                    128, 149
                ],
                "accessibility_score": 0.85
            },
            
            # 7. CHAMBERÍ
            "Chamberí": {
                "metro_stations": [
                    "Bilbao", "Iglesia", "Quevedo",
                    "Canal", "Islas Filipinas",
                    "Alonso Cano", "Ríos Rosas",
                    "San Bernardo"
                ],
                "cercanias_stations": [
                    "Nuevos Ministerios"
                ],
                "bus_lines": [
                    3, 12, 16, 21, 37,
                    40, 61
                ],
                "accessibility_score": 0.9
            },
            
            # 8. FUENCARRAL-EL PARDO
            "Fuencarral-El Pardo": {
                "metro_stations": [
                    "Fuencarral", "Tres Olivos",
                    "Montecarmelo", "Mirasierra",
                    "Paco de Lucía", "Barrio del Pilar",
                    "Begoña", "Herrera Oria",
                    "Lacoma", "Avenida de la Ilustración",
                    "Pitis"
                ],
                "cercanias_stations": [
                    "Pitis", "Ramón y Cajal", "Mirasierra-Paco de Lucía"
                ],
                "bus_lines": [
                    49, 64, 67, 83,
                    126, 128, 133, 134, 135, 147
                ],
                "accessibility_score": 0.8
            },
            
            # 9. MONCLOA-ARAVACA
            "Moncloa-Aravaca": {
                "metro_stations": [
                    "Moncloa", "Argüelles",
                    "Ciudad Universitaria",
                    "Vicente Aleixandre", "Islas Filipinas",
                    "Lago", "Batán",
                    "Príncipe Pío"
                ],
                "cercanias_stations": [
                    "Príncipe Pío", "Aravaca"
                ],
                "bus_lines": [
                    1, 16, 21, 44,
                    46, 61, 82, 83,
                    132, 160, 161, 162
                ],
                "accessibility_score": 0.88
            },
            
            # 10. LATINA
            "Latina": {
                "metro_stations": [
                    "Alto de Extremadura", "Lucero",
                    "Batán", "Campamento",
                    "Casa de Campo",
                    "Aluche", "Eugenia de Montijo",
                    "Carpetana", "Laguna"
                ],
                "cercanias_stations": [
                    "Laguna", "Maestra Justa Freire-Polideportivo Aluche",
                    "Aluche"
                ],
                "bus_lines": [
                    17, 25, 31, 39, 55,
                    65, 121, 131, 138
                ],
                "accessibility_score": 0.8
            },
            
            # 11. CARABANCHEL
            "Carabanchel": {
                "metro_stations": [
                    "Carabanchel", "Carabanchel Alto",
                    "Eugenia de Montijo", "Oporto",
                    "Vista Alegre", "Opañel",
                    "Pan Bendito"
                ],
                "cercanias_stations": [],
                "bus_lines": [
                    34, 35, 47, 81,
                    108, 118, 121
                ],
                "accessibility_score": 0.75
            },
            
            # 12. USERA
            "Usera": {
                "metro_stations": [
                    "Plaza Elíptica", "Usera",
                    "Almendrales", "Hospital 12 de Octubre",
                    "San Fermín-Orcasur",
                    "Legazpi"   # límite con Arganzuela, pero clave para el distrito
                ],
                "cercanias_stations": [
                    "Doce de Octubre"
                ],
                "bus_lines": [
                    6, 18, 22, 23, 47,
                    59, 60, 78, 81
                ],
                "accessibility_score": 0.7
            },
            
            # 13. PUENTE DE VALLECAS
            "Puente de Vallecas": {
                "metro_stations": [
                    "Puente de Vallecas", "Nueva Numancia",
                    "Portazgo", "Buenos Aires",
                    "Alto del Arenal"
                ],
                "cercanias_stations": [
                    "El Pozo", "Asamblea de Madrid-Entrevías"
                ],
                "bus_lines": [
                    10, 24, 37, 54,
                    57, 58, 111, 102, 103, 136, 310
                ],
                "accessibility_score": 0.7
            },
            
            # 14. MORATALAZ
            "Moratalaz": {
                "metro_stations": [
                    "Vinateros", "Artilleros",
                    "Pavones", "Estrella"
                ],
                "cercanias_stations": [],
                "bus_lines": [
                    20, 30, 32, 71, 100, 140
                ],
                "accessibility_score": 0.75
            },
            
            # 15. CIUDAD LINEAL
            "Ciudad Lineal": {
                "metro_stations": [
                    "Ventas", "La Elipa",
                    "El Carmen", "Quintana",
                    "Pueblo Nuevo", "Ciudad Lineal",
                    "Ascao", "Suanzes", "Torre Arias",
                    "Barrio de la Concepción"
                ],
                "cercanias_stations": [],
                "bus_lines": [
                    4, 21, 38, 48,
                    70, 106, 113, 146
                ],
                "accessibility_score": 0.8
            },
            
            # 16. HORTALEZA
            "Hortaleza": {
                "metro_stations": [
                    "Pinar del Rey", "Hortaleza",
                    "Parque de Santa María", "San Lorenzo",
                    "Mar de Cristal", "Canillas", "Esperanza",
                    "Manoteras"
                ],
                "cercanias_stations": [
                    "Fuente de la Mora"
                ],
                "bus_lines": [
                    7, 9, 29, 72,
                    73, 120, 122, 125, 172
                ],
                "accessibility_score": 0.8
            },
            
            # 17. VILLAVERDE
            "Villaverde": {
                "metro_stations": [
                    "Villaverde Alto", "San Cristóbal",
                    "Ciudad de los Ángeles"
                ],
                "cercanias_stations": [
                    "Villaverde Alto", "Villaverde Bajo"
                ],
                "bus_lines": [
                    22, 76, 79, 85, 86,
                    123, 130, 131
                ],
                "accessibility_score": 0.72
            },
            
            # 18. VILLA DE VALLECAS
            "Villa de Vallecas": {
                "metro_stations": [
                    "Sierra de Guadalupe", "Congosto",
                    "La Gavia", "Las Suertes",
                    "Valdecarros", "Villa de Vallecas"
                ],
                "cercanias_stations": [
                    "Vallecas", "Santa Eugenia",
                    "Sierra de Guadalupe"  # Correspondencia Metro+Cercanías
                ],
                "bus_lines": [
                    54, 58, 63, 130,
                    142, 145
                ],
                "accessibility_score": 0.7
            },
            
            # 19. VICÁLVARO
            "Vicálvaro": {
                "metro_stations": [
                    "Vicálvaro", "San Cipriano",
                    "Puerta de Arganda"
                ],
                "cercanias_stations": [
                    "Vicálvaro"
                ],
                "bus_lines": [
                    4, 71, 100, 106, 130
                ],
                "accessibility_score": 0.7
            },
            
            # 20. SAN BLAS-CANILLEJAS
            "San Blas-Canillejas": {
                "metro_stations": [
                    "San Blas", "Simancas",
                    "Las Musas", "Las Rosas",
                    "Canillejas", "Torre Arias",
                    "Suanzes", "Ciudad Lineal", "Avenida de Guadalajara"
                ],
                "cercanias_stations": [],
                "bus_lines": [
                    28, 38, 48, 77,
                    104, 105, 109, 114, 140, 153
                ],
                "accessibility_score": 0.75
            },
            
            # 21. BARAJAS
            "Barajas": {
                "metro_stations": [
                    "Barajas",
                    "Aeropuerto T1-T2-T3",
                    "Aeropuerto T4",
                    "Alameda de Osuna"
                ],
                "cercanias_stations": [
                    "Aeropuerto T4"
                ],
                "bus_lines": [
                    101, 105, 115, 151  # más Exprés Aeropuerto (no numerado como línea normal)
                ],
                "accessibility_score": 0.8
            }
        }
    
    def _calculate_district_metrics(self, district: str) -> Dict:
        """
        Calcular métricas de movilidad para un distrito específico
        
        Args:
            district: Nombre del distrito
            
        Returns:
            Diccionario con métricas del distrito
        """
        if district not in self.transport_stations:
            return {}
        
        district_info = self.transport_stations[district]
        metro_count = len(district_info['metro_stations'])
        bus_count = len(district_info['bus_lines'])
        cercanias_count = len(district_info['cercanias_stations'])
        total_stations = metro_count + bus_count + cercanias_count
        
        # Calcular tiempo promedio de viaje (más estaciones = mejor conectividad = menor tiempo)
        base_travel_time = 30.0
        travel_time_factor = max(1.0, metro_count / 5.0)
        avg_travel_time = base_travel_time / travel_time_factor
        
        # Calcular accesibilidad (usar el score definido o calcularlo)
        accessibility_score = district_info.get('accessibility_score', min(1.0, total_stations / 50.0))
        
        # Calcular diversidad de transporte
        all_stations = district_info['metro_stations'] + district_info['cercanias_stations']
        transport_diversity = len(set(all_stations)) / 10.0
        
        # Calcular conectividad
        connectivity_score = (metro_count * 0.4 + bus_count * 0.3 + cercanias_count * 0.3)
        
        return {
            'distrito': district,
            'metro_stations_count': metro_count,
            'bus_lines_count': bus_count,
            'cercanias_stations_count': cercanias_count,
            'total_transport_stations': total_stations,
            'avg_travel_time_minutes': round(avg_travel_time, 2),
            'accessibility_score': round(accessibility_score, 2),
            'metro_density': round(metro_count / 10.0, 2),
            'bus_density': round(bus_count / 10.0, 2),
            'transport_diversity': round(transport_diversity, 2),
            'connectivity_score': round(connectivity_score, 2)
        }
    
    def get_mobility_data_all_districts(self) -> pd.DataFrame:
        """
        Obtener datos de movilidad para todos los distritos (21 registros, uno por distrito)
        
        Returns:
            DataFrame con datos de movilidad agregados por distrito
        """
        try:
            logger.info("Generando datos de movilidad basados en estructura de transporte")
            mobility_data = []
            
            for district in self.transport_stations.keys():
                metrics = self._calculate_district_metrics(district)
                if metrics:
                    mobility_data.append(metrics)
            
            df = pd.DataFrame(mobility_data)
            logger.info(f"✅ Datos generados para {len(df)} distritos")
            return df
            
        except Exception as e:
            logger.error(f"Error al obtener datos de movilidad: {str(e)}")
            return pd.DataFrame()
    
    def calculate_mobility_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcular métricas adicionales de movilidad por distrito
        Los datos ya deben venir agregados por distrito (de get_mobility_data_all_districts)
        
        Args:
            df: DataFrame con datos de movilidad por distrito
            
        Returns:
            DataFrame con métricas completas
        """
        if df.empty:
            return pd.DataFrame()
        
        metrics = df.copy()
        
        # Asegurar que todas las columnas necesarias existan
        required_cols = {
            'metro_stations_count': 0,
            'bus_lines_count': 0,
            'cercanias_stations_count': 0,
            'accessibility_score': 0.5
        }
        
        for col, default_value in required_cols.items():
            if col not in metrics.columns:
                metrics[col] = default_value
        
        # Calcular métricas adicionales si no existen
        if 'total_transport_stations' not in metrics.columns:
            metrics['total_transport_stations'] = (
                metrics['metro_stations_count'] + 
                metrics['bus_lines_count'] + 
                metrics['cercanias_stations_count']
            )
        
        if 'connectivity_score' not in metrics.columns:
            metrics['connectivity_score'] = (
                metrics['metro_stations_count'] * 0.4 + 
                metrics['bus_lines_count'] * 0.3 + 
                metrics['cercanias_stations_count'] * 0.3
            ).round(2)
        
        return metrics
    
    def get_connectivity_analysis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Análisis de conectividad por distrito
        
        Args:
            df: DataFrame con datos de movilidad por distrito
            
        Returns:
            DataFrame con análisis de conectividad y clasificación
        """
        if df.empty:
            return pd.DataFrame()
        
        # Usar connectivity_score si existe, sino calcularlo
        analysis = df.copy()
        
        if 'connectivity_score' not in analysis.columns:
            analysis['connectivity_score'] = (
                analysis['metro_stations_count'] * 0.4 + 
                analysis['bus_lines_count'] * 0.3 + 
                analysis['cercanias_stations_count'] * 0.3
            ).round(2)
        
        # Clasificar conectividad
        def classify_connectivity(score):
            if score >= 20:
                return "Excelente"
            elif score >= 15:
                return "Buena"
            elif score >= 10:
                return "Moderada"
            else:
                return "Baja"
        
        analysis['connectivity_level'] = analysis['connectivity_score'].apply(classify_connectivity)
        
        # Renombrar columnas para claridad
        analysis = analysis.rename(columns={
            'metro_stations_count': 'metro_stations',
            'bus_lines_count': 'bus_lines',
            'cercanias_stations_count': 'cercanias_stations',
            'total_transport_stations': 'total_stations'
        })
        
        return analysis
    
    def save_mobility_data(self, df: pd.DataFrame, filename: str = "mobility_data_madrid.csv"):
        """
        Guardar datos de movilidad en archivo CSV
        
        Args:
            df: DataFrame con datos de movilidad
            filename: Nombre del archivo
        """
        from utils.file_utils import save_dataframe_to_csv
        try:
            save_dataframe_to_csv(df, filename, subdirectory="processed")
        except ValueError:
            logger.warning("No hay datos para guardar")

def main():
    """
    Función principal para ejecutar la recopilación de datos de movilidad
    """
    # Crear instancia del colector
    collector = MobilityDataCollector()
    
    # Obtener datos para todos los distritos (21 registros, uno por distrito)
    mobility_df = collector.get_mobility_data_all_districts()
    
    if not mobility_df.empty:
        # Calcular métricas
        metrics_df = collector.calculate_mobility_metrics(mobility_df)
        connectivity_df = collector.get_connectivity_analysis(mobility_df)
        
        # Guardar datos
        collector.save_mobility_data(mobility_df, "mobility_data_madrid.csv")
        collector.save_mobility_data(metrics_df, "mobility_metrics_madrid.csv")
        collector.save_mobility_data(connectivity_df, "mobility_connectivity_madrid.csv")
        
        print(f"✅ Datos de movilidad recopilados para {len(mobility_df)} registros")
        print(f"✅ Métricas calculadas para {len(metrics_df)} distritos")
        print(f"✅ Análisis de conectividad para {len(connectivity_df)} distritos")
    else:
        print("❌ No se pudieron recopilar datos de movilidad")

if __name__ == "__main__":
    main()
