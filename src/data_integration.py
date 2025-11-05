"""
Módulo para integrar todos los datos adicionales en el análisis principal
Combina datos meteorológicos, precios de electricidad, calidad del aire y movilidad
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import logging
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Importar los colectores de datos
from weather_data_collector import WeatherDataCollector
from electricity_prices_collector import ElectricityPricesCollector
from air_quality_collector import AirQualityCollector
from mobility_data_collector import MobilityDataCollector

# Configurar logging usando configuración centralizada
from config.logging_config import get_logger
logger = get_logger(__name__)

class DataIntegration:
    """
    Clase para integrar todos los datos adicionales en el análisis principal
    """
    
    def __init__(self, openweather_api_key: Optional[str] = None):
        """
        Inicializar el integrador de datos
        
        Args:
            openweather_api_key: API key de OpenWeatherMap (opcional)
        """
        self.openweather_api_key = openweather_api_key
        
        # Inicializar colectores
        self.weather_collector = WeatherDataCollector(openweather_api_key)
        self.electricity_collector = ElectricityPricesCollector()
        self.air_quality_collector = AirQualityCollector()
        self.mobility_collector = MobilityDataCollector()
        
        # Importar mapeo de distritos desde utilidades centralizadas
        from utils.madrid_districts import get_district_mapping
        self.district_mapping = get_district_mapping()
    
    def collect_all_additional_data(self, days_back: int = 30) -> Dict[str, pd.DataFrame]:
        """
        Recopilar todos los datos adicionales
        
        Args:
            days_back: Número de días hacia atrás para recopilar datos
            
        Returns:
            Diccionario con todos los DataFrames de datos adicionales
        """
        logger.info("🔄 Iniciando recopilación de datos adicionales...")
        
        all_data = {}
        
        try:
            # 1. Datos meteorológicos
            logger.info("🌤️ Recopilando datos meteorológicos...")
            weather_df = self.weather_collector.get_weather_data_all_districts(days_back=days_back)
            if not weather_df.empty:
                all_data['weather'] = weather_df
                logger.info(f"✅ Datos meteorológicos: {len(weather_df)} registros")
            else:
                logger.warning("⚠️ No se obtuvieron datos meteorológicos")
            
            # 2. Datos de precios de electricidad
            logger.info("⚡ Recopilando datos de precios de electricidad...")
            electricity_df = self.electricity_collector.get_historical_prices(days_back=days_back)
            if not electricity_df.empty:
                all_data['electricity'] = electricity_df
                logger.info(f"✅ Datos de precios: {len(electricity_df)} registros")
            else:
                logger.warning("⚠️ No se obtuvieron datos de precios")
            
            # 3. Datos de calidad del aire
            logger.info("🌬️ Recopilando datos de calidad del aire...")
            air_quality_df = self.air_quality_collector.get_air_quality_all_districts(year=2023)
            if not air_quality_df.empty:
                all_data['air_quality'] = air_quality_df
                logger.info(f"✅ Datos de calidad del aire: {len(air_quality_df)} registros")
            else:
                logger.warning("⚠️ No se obtuvieron datos de calidad del aire")
            
            # 4. Datos de movilidad
            logger.info("🚌 Recopilando datos de movilidad...")
            mobility_df = self.mobility_collector.get_mobility_data_all_districts()
            if not mobility_df.empty:
                all_data['mobility'] = mobility_df
                logger.info(f"✅ Datos de movilidad: {len(mobility_df)} registros")
            else:
                logger.warning("⚠️ No se obtuvieron datos de movilidad")
            
            return all_data
            
        except Exception as e:
            logger.error(f"❌ Error en la recopilación de datos: {str(e)}")
            return {}
    
    def calculate_additional_metrics(self, all_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Calcular métricas adicionales para cada tipo de datos
        
        Args:
            all_data: Diccionario con todos los DataFrames de datos
            
        Returns:
            Diccionario con métricas calculadas
        """
        logger.info("📊 Calculando métricas adicionales...")
        
        metrics = {}
        
        try:
            # 1. Métricas meteorológicas
            if 'weather' in all_data and not all_data['weather'].empty:
                weather_metrics = self.weather_collector.calculate_weather_metrics(all_data['weather'])
                metrics['weather_metrics'] = weather_metrics
                logger.info("✅ Métricas meteorológicas calculadas")
            
            # 2. Métricas de precios de electricidad
            if 'electricity' in all_data and not all_data['electricity'].empty:
                electricity_metrics = self.electricity_collector.calculate_price_metrics(all_data['electricity'])
                metrics['electricity_metrics'] = electricity_metrics
                logger.info("✅ Métricas de precios calculadas")
            
            # 3. Métricas de calidad del aire
            if 'air_quality' in all_data and not all_data['air_quality'].empty:
                air_quality_metrics = self.air_quality_collector.calculate_air_quality_metrics(all_data['air_quality'])
                health_metrics = self.air_quality_collector.get_health_impact_assessment(all_data['air_quality'])
                metrics['air_quality_metrics'] = air_quality_metrics
                metrics['health_metrics'] = health_metrics
                logger.info("✅ Métricas de calidad del aire calculadas")
            
            # 4. Métricas de movilidad
            if 'mobility' in all_data and not all_data['mobility'].empty:
                mobility_metrics = self.mobility_collector.calculate_mobility_metrics(all_data['mobility'])
                connectivity_metrics = self.mobility_collector.get_connectivity_analysis(all_data['mobility'])
                metrics['mobility_metrics'] = mobility_metrics
                metrics['connectivity_metrics'] = connectivity_metrics
                logger.info("✅ Métricas de movilidad calculadas")
            
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Error calculando métricas: {str(e)}")
            return {}
    
    def integrate_with_main_data(self, main_df: pd.DataFrame, additional_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Integrar datos adicionales con el dataset principal
        
        Args:
            main_df: DataFrame principal con datos de consumo, demografía y rentas
            additional_data: Diccionario con datos adicionales
            
        Returns:
            DataFrame integrado
        """
        logger.info("🔗 Integrando datos adicionales con dataset principal...")
        
        try:
            # Crear copia del DataFrame principal
            integrated_df = main_df.copy()
            
            # 1. Integrar datos meteorológicos
            if 'weather' in additional_data and not additional_data['weather'].empty:
                weather_metrics = self.weather_collector.calculate_weather_metrics(additional_data['weather'])
                integrated_df = self._merge_metrics(integrated_df, weather_metrics, 'weather')
                logger.info("✅ Datos meteorológicos integrados")
            
            # 2. Integrar datos de precios de electricidad
            if 'electricity' in additional_data and not additional_data['electricity'].empty:
                electricity_metrics = self.electricity_collector.calculate_price_metrics(additional_data['electricity'])
                integrated_df = self._merge_metrics(integrated_df, electricity_metrics, 'electricity')
                logger.info("✅ Datos de precios integrados")
            
            # 3. Integrar datos de calidad del aire
            if 'air_quality' in additional_data and not additional_data['air_quality'].empty:
                air_quality_metrics = self.air_quality_collector.calculate_air_quality_metrics(additional_data['air_quality'])
                integrated_df = self._merge_metrics(integrated_df, air_quality_metrics, 'air_quality')
                logger.info("✅ Datos de calidad del aire integrados")
            
            # 4. Integrar datos de movilidad
            if 'mobility' in additional_data and not additional_data['mobility'].empty:
                mobility_metrics = self.mobility_collector.calculate_mobility_metrics(additional_data['mobility'])
                integrated_df = self._merge_metrics(integrated_df, mobility_metrics, 'mobility')
                logger.info("✅ Datos de movilidad integrados")
            
            # 5. Calcular métricas compuestas
            integrated_df = self._calculate_composite_metrics(integrated_df)
            
            logger.info(f"✅ Integración completada: {len(integrated_df)} distritos, {len(integrated_df.columns)} variables")
            return integrated_df
            
        except Exception as e:
            logger.error(f"❌ Error en la integración: {str(e)}")
            return main_df
    
    def _merge_metrics(self, main_df: pd.DataFrame, metrics_df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """
        Fusionar métricas con el DataFrame principal
        
        Args:
            main_df: DataFrame principal
            metrics_df: DataFrame con métricas
            data_type: Tipo de datos (weather, electricity, etc.)
            
        Returns:
            DataFrame fusionado
        """
        try:
            # Asegurar que ambos DataFrames tengan la columna 'distrito'
            if 'distrito' in metrics_df.columns:
                # Renombrar columnas para evitar conflictos
                metrics_renamed = metrics_df.copy()
                for col in metrics_renamed.columns:
                    if col != 'distrito':
                        metrics_renamed = metrics_renamed.rename(columns={col: f"{data_type}_{col}"})
                
                # Fusionar por distrito
                merged_df = main_df.merge(metrics_renamed, on='distrito', how='left')
                return merged_df
            else:
                logger.warning(f"No se encontró columna 'distrito' en métricas de {data_type}")
                return main_df
                
        except Exception as e:
            logger.error(f"Error fusionando métricas de {data_type}: {str(e)}")
            return main_df
    
    def _calculate_composite_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcular métricas compuestas que combinan diferentes tipos de datos
        
        Args:
            df: DataFrame integrado
            
        Returns:
            DataFrame con métricas compuestas
        """
        try:
            # 1. Índice de sostenibilidad ambiental
            if 'air_quality_ICA_mean' in df.columns and 'weather_temperatura_media_mean' in df.columns:
                # Normalizar métricas (0-1)
                ica_norm = (df['air_quality_ICA_mean'] - df['air_quality_ICA_mean'].min()) / (df['air_quality_ICA_mean'].max() - df['air_quality_ICA_mean'].min())
                temp_norm = 1 - (df['weather_temperatura_media_mean'] - df['weather_temperatura_media_mean'].min()) / (df['weather_temperatura_media_mean'].max() - df['weather_temperatura_media_mean'].min())
                
                df['sustainability_index'] = (ica_norm + temp_norm) / 2
                df['sustainability_index'] = df['sustainability_index'].round(3)
            
            # 2. Índice de eficiencia energética
            if 'electricity_precio_distrito_mean' in df.columns and 'p50' in df.columns:
                # Relación entre consumo y precio
                df['energy_efficiency_index'] = (df['p50'] / df['electricity_precio_distrito_mean']).round(3)
            
            # 3. Índice de accesibilidad urbana
            if 'mobility_accessibility_score_mean' in df.columns and 'mobility_connectivity_score' in df.columns:
                df['urban_accessibility_index'] = (df['mobility_accessibility_score_mean'] + df['mobility_connectivity_score'] / 100) / 2
                df['urban_accessibility_index'] = df['urban_accessibility_index'].round(3)
            
            # 4. Índice de calidad de vida
            quality_indicators = []
            if 'sustainability_index' in df.columns:
                quality_indicators.append(df['sustainability_index'])
            if 'urban_accessibility_index' in df.columns:
                quality_indicators.append(df['urban_accessibility_index'])
            if 'renta neta media por persona' in df.columns:
                renta_norm = (df['renta neta media por persona'] - df['renta neta media por persona'].min()) / (df['renta neta media por persona'].max() - df['renta neta media por persona'].min())
                quality_indicators.append(renta_norm)
            
            if quality_indicators:
                df['quality_of_life_index'] = pd.concat(quality_indicators, axis=1).mean(axis=1).round(3)
            
            return df
            
        except Exception as e:
            logger.error(f"Error calculando métricas compuestas: {str(e)}")
            return df
    
    def save_integrated_data(self, df: pd.DataFrame, filename: str = "integrated_data_madrid.csv"):
        """
        Guardar datos integrados en archivo CSV
        
        Args:
            df: DataFrame integrado
            filename: Nombre del archivo
        """
        try:
            filepath = os.path.join("data", "processed", filename)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            df.to_csv(filepath, index=False)
            logger.info(f"✅ Datos integrados guardados en {filepath}")
        except Exception as e:
            logger.error(f"❌ Error guardando datos integrados: {str(e)}")
    
    def generate_integration_report(self, integrated_df: pd.DataFrame) -> Dict:
        """
        Generar reporte de integración de datos
        
        Args:
            integrated_df: DataFrame integrado
            
        Returns:
            Diccionario con reporte de integración
        """
        try:
            report = {
                'total_districts': len(integrated_df),
                'total_variables': len(integrated_df.columns),
                'data_types_integrated': [],
                'missing_data_summary': {},
                'new_metrics_created': []
            }
            
            # Identificar tipos de datos integrados
            if any('weather_' in col for col in integrated_df.columns):
                report['data_types_integrated'].append('weather')
            if any('electricity_' in col for col in integrated_df.columns):
                report['data_types_integrated'].append('electricity')
            if any('air_quality_' in col for col in integrated_df.columns):
                report['data_types_integrated'].append('air_quality')
            if any('mobility_' in col for col in integrated_df.columns):
                report['data_types_integrated'].append('mobility')
            
            # Resumen de datos faltantes
            missing_data = integrated_df.isnull().sum()
            report['missing_data_summary'] = missing_data[missing_data > 0].to_dict()
            
            # Identificar nuevas métricas creadas
            new_metrics = ['sustainability_index', 'energy_efficiency_index', 'urban_accessibility_index', 'quality_of_life_index']
            report['new_metrics_created'] = [metric for metric in new_metrics if metric in integrated_df.columns]
            
            return report
            
        except Exception as e:
            logger.error(f"Error generando reporte: {str(e)}")
            return {}

def main():
    """
    Función principal para ejecutar la integración de datos
    """
    # Obtener API key de variable de entorno
    api_key = os.getenv('OPENWEATHER_API_KEY')
    
    # Crear instancia del integrador
    integrator = DataIntegration(api_key)
    
    # Recopilar todos los datos adicionales
    additional_data = integrator.collect_all_additional_data(days_back=30)
    
    if additional_data:
        # Calcular métricas adicionales
        additional_metrics = integrator.calculate_additional_metrics(additional_data)
        
        # Guardar datos adicionales
        for data_type, df in additional_data.items():
            integrator.weather_collector.save_weather_data(df, f"{data_type}_data_madrid.csv")
        
        for data_type, df in additional_metrics.items():
            integrator.weather_collector.save_weather_data(df, f"{data_type}_madrid.csv")
        
        print(f"✅ Datos adicionales recopilados y procesados")
        print(f"✅ Tipos de datos: {list(additional_data.keys())}")
        print(f"✅ Métricas calculadas: {list(additional_metrics.keys())}")
    else:
        print("❌ No se pudieron recopilar datos adicionales")

if __name__ == "__main__":
    main()
