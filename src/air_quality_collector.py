"""
Módulo para recopilar datos de calidad del aire de Madrid por distritos
Utiliza datos del Ayuntamiento de Madrid y Red de Calidad del Aire
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
from typing import Dict, List, Optional
import logging
import math

# Configurar logging usando configuración centralizada
from config.logging_config import get_logger
logger = get_logger(__name__)

class AirQualityCollector:
    """
    Clase para recopilar datos de calidad del aire de Madrid por distritos
    """
    
    def __init__(self):
        """
        Inicializar el colector de datos de calidad del aire
        """
        # URLs de APIs del Ayuntamiento de Madrid para datos de calidad del aire
        # API en tiempo real (actualizada cada 20 minutos)
        self.madrid_air_realtime_api = "https://datos.madrid.es/egob/catalogo/212531-7916318-calidad-aire-tiempo-real.json"
        # URL para descargar datos CSV
        self.madrid_csv_download = "https://datos.madrid.es/egob/catalogo/212531-7916318-calidad-aire-tiempo-real.csv"
        
        # Mapeo básico de estaciones a distritos (para cuando se obtienen datos por estación)
        self.station_to_district = {
            "28079001": "Centro",
            "28079002": "Centro",
            "28079004": "Centro",
            "28079003": "Arganzuela",
            "28079035": "Arganzuela",
            "28079036": "Retiro",
            "28079008": "Retiro",
            "28079009": "Salamanca",
            "28079038": "Chamartín",
            "28079010": "Chamartín",
            "28079011": "Chamberí",
            "28079012": "Moncloa-Aravaca",
            "28079013": "Usera",
            "28079014": "Puente de Vallecas",
            "28079015": "Ciudad Lineal",
            "28079016": "Hortaleza",
            "28079017": "Villaverde",
            "28079018": "San Blas-Canillejas",
        }
        
        # Estaciones de calidad del aire por distrito y factores de contaminación
        # Factores ajustados: pequeños multiplicadores, no cambios brutales
        self.air_stations = {
            "Centro": {"district_factor": 1.10},
            "Arganzuela": {"district_factor": 1.05},
            "Retiro": {"district_factor": 0.95},
            "Salamanca": {"district_factor": 1.05},
            "Chamartín": {"district_factor": 0.95},
            "Tetuán": {"district_factor": 1.00},
            "Chamberí": {"district_factor": 1.00},
            "Fuencarral-El Pardo": {"district_factor": 0.85},
            "Moncloa-Aravaca": {"district_factor": 0.90},
            "Latina": {"district_factor": 0.95},
            "Carabanchel": {"district_factor": 1.00},
            "Usera": {"district_factor": 1.05},
            "Puente de Vallecas": {"district_factor": 1.10},
            "Moratalaz": {"district_factor": 0.95},
            "Ciudad Lineal": {"district_factor": 0.95},
            "Hortaleza": {"district_factor": 0.90},
            "Villaverde": {"district_factor": 1.05},
            "Villa de Vallecas": {"district_factor": 1.05},
            "Vicálvaro": {"district_factor": 0.95},
            "San Blas-Canillejas": {"district_factor": 0.95},
            "Barajas": {"district_factor": 0.85}
        }
        
        # Límites de calidad del aire aproximados a normativa UE/OMS (µg/m³, CO en mg/m³)
        # Estos no son los oficiales al 100%, pero están en rangos razonables
        self.air_quality_limits = {
            'NO2': {'bueno': 40, 'moderado': 100, 'malo': 200, 'muy_malo': 400},
            'PM10': {'bueno': 20, 'moderado': 40, 'malo': 50, 'muy_malo': 100},
            'PM2.5': {'bueno': 10, 'moderado': 25, 'malo': 35, 'muy_malo': 75},
            'O3': {'bueno': 120, 'moderado': 180, 'malo': 240, 'muy_malo': 320},
            'SO2': {'bueno': 125, 'moderado': 350, 'malo': 500, 'muy_malo': 750},
            'CO': {'bueno': 2, 'moderado': 4, 'malo': 8, 'muy_malo': 15}  # mg/m³ aprox
        }
        
        # Rangos de referencia (valores medios y extremos razonables para Madrid)
        # Aproximados a partir de estudios y datos históricos para PM2.5, PM10, NO2, O3, etc.
        self.real_reference_ranges = {
            'NO2': {'mean': 35.0, 'min': 10.0, 'max': 80.0},
            'PM10': {'mean': 22.0, 'min': 5.0, 'max': 60.0},
            'PM2.5': {'mean': 10.0, 'min': 3.0, 'max': 35.0},
            'O3': {'mean': 70.0, 'min': 20.0, 'max': 160.0},
            'SO2': {'mean': 4.0, 'min': 0.0, 'max': 20.0},
            'CO': {'mean': 0.4, 'min': 0.05, 'max': 2.0}  # mg/m³
        }
    
    def get_air_quality_from_csv(self, filepath: Optional[str] = None) -> pd.DataFrame:
        """
        Intentar descargar y procesar datos CSV del Ayuntamiento de Madrid
        
        Args:
            filepath: Ruta local del archivo CSV (opcional, si no se proporciona intenta descargarlo)
            
        Returns:
            DataFrame con datos de calidad del aire
        """
        try:
            if filepath and os.path.exists(filepath):
                logger.info(f"Leyendo datos desde archivo local: {filepath}")
                df = pd.read_csv(filepath, encoding='utf-8')
                # Procesar y normalizar datos
                return self._process_madrid_csv_data(df)
            else:
                logger.info("Intentando descargar datos CSV del Ayuntamiento de Madrid...")
                
                # Intentar descargar desde la URL configurada
                csv_urls = [self.madrid_csv_download]
                
                for url in csv_urls:
                    try:
                        logger.info(f"Intentando descargar desde: {url}")
                        response = requests.get(url, timeout=30, allow_redirects=True)
                        
                        # Verificar que la respuesta sea válida
                        if response.status_code == 200 and len(response.content) > 100:
                            # Verificar que no esté intentando redirigir a una URL problemática
                            final_url = response.url
                            if 'mambiente.madrid.es' in final_url:
                                logger.warning(f"⚠️ La URL redirige a un dominio no disponible: {final_url}")
                                continue
                            
                            # Guardar temporalmente
                            temp_file = "temp_air_quality.csv"
                            with open(temp_file, 'wb') as f:
                                f.write(response.content)
                            
                            # Verificar que el archivo tenga contenido CSV válido
                            try:
                                df = pd.read_csv(temp_file, encoding='utf-8', nrows=5)  # Leer solo 5 filas para verificar
                                if df.empty:
                                    os.remove(temp_file)
                                    continue
                                
                                # Si es válido, leer todo el archivo
                                df = pd.read_csv(temp_file, encoding='utf-8')
                                os.remove(temp_file)
                                
                                # Procesar y normalizar datos
                                logger.info(f"✅ CSV descargado correctamente: {len(df)} filas")
                                return self._process_madrid_csv_data(df)
                            except Exception as e:
                                if os.path.exists(temp_file):
                                    os.remove(temp_file)
                                logger.warning(f"El archivo descargado no es un CSV válido: {str(e)}")
                                continue
                        else:
                            logger.debug(f"URL no disponible o respuesta vacía: {url} (status: {response.status_code})")
                            continue
                    except requests.exceptions.RequestException as e:
                        logger.debug(f"Error de conexión con {url}: {str(e)}")
                        continue
                    except Exception as e:
                        logger.debug(f"Error al procesar {url}: {str(e)}")
                        continue
                
                # Si ninguna URL funcionó, devolver DataFrame vacío
                logger.warning("⚠️ No se pudieron descargar datos CSV desde ninguna URL disponible")
                return pd.DataFrame()
                
        except Exception as e:
            logger.warning(f"Error general al procesar datos CSV: {str(e)}")
            return pd.DataFrame()
    
    def _process_madrid_csv_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Procesar datos CSV del Ayuntamiento de Madrid y normalizarlos
        
        Args:
            df: DataFrame con datos CSV sin procesar
            
        Returns:
            DataFrame normalizado con datos de calidad del aire
        """
        air_data = []
        
        # Normalizar nombres de columnas (pueden variar según el formato del CSV)
        df.columns = df.columns.str.strip().str.lower()
        
        # Intentar identificar columnas relevantes
        date_col = None
        station_col = None
        pollutant_cols = {}
        
        for col in df.columns:
            col_lower = col.lower()
            if 'fecha' in col_lower or 'date' in col_lower or 'time' in col_lower:
                date_col = col
            elif 'estacion' in col_lower or 'station' in col_lower or 'punto' in col_lower:
                station_col = col
            elif 'no2' in col_lower:
                pollutant_cols['NO2'] = col
            elif 'pm10' in col_lower:
                pollutant_cols['PM10'] = col
            elif 'pm2' in col_lower or 'pm25' in col_lower:
                pollutant_cols['PM2.5'] = col
            elif 'o3' in col_lower or 'ozono' in col_lower:
                pollutant_cols['O3'] = col
            elif 'so2' in col_lower:
                pollutant_cols['SO2'] = col
            elif 'co' in col_lower:
                pollutant_cols['CO'] = col
        
        if not date_col or not station_col:
            logger.warning("No se pudieron identificar columnas de fecha o estación en el CSV")
            return pd.DataFrame()
        
        # Procesar cada fila
        for _, row in df.iterrows():
            try:
                # Obtener fecha
                fecha_str = str(row[date_col])
                fecha = pd.to_datetime(fecha_str, errors='coerce')
                if pd.isna(fecha):
                    continue
                
                # Obtener estación y determinar distrito
                station_name = str(row[station_col])
                district = self._extract_district_from_station_name(station_name)
                if not district:
                    continue
                    
                # Extraer contaminantes
                pollutants = {}
                for pollutant, col in pollutant_cols.items():
                    try:
                        value = pd.to_numeric(row[col], errors='coerce')
                        if pd.notna(value) and value >= 0:
                            pollutants[pollutant] = float(value)
                    except:
                        pass
                        
                if pollutants:
                    ica = self._calculate_air_quality_index(pollutants)
                    air_data.append({
                        'fecha': fecha.strftime('%Y-%m-%d'),
                        'distrito': district,
                        'NO2': pollutants.get('NO2'),
                        'PM10': pollutants.get('PM10'),
                        'PM2.5': pollutants.get('PM2.5'),
                        'O3': pollutants.get('O3'),
                        'SO2': pollutants.get('SO2'),
                        'CO': pollutants.get('CO'),
                        'ICA': ica.get('index'),
                        'calidad': ica.get('quality'),
                        'recomendacion': ica.get('recommendation'),
                        'estacion_nombre': station_name
                    })
            except Exception as e:
                logger.debug(f"Error procesando fila: {str(e)}")
                continue
                
        if air_data:
            return pd.DataFrame(air_data)
        return pd.DataFrame()
    
    def _extract_district_from_station_name(self, station_name: str) -> Optional[str]:
        """
        Extraer distrito del nombre de la estación
        
        Args:
            station_name: Nombre de la estación
            
        Returns:
            Nombre del distrito o None
        """
        station_name_lower = station_name.lower()
        
        # Mapeo de palabras clave a distritos
        keywords_districts = {
            'centro': 'Centro',
            'castilla': 'Chamartín',
            'retiro': 'Retiro',
            'vallecas': 'Puente de Vallecas',
            'villaverde': 'Villaverde',
            'barajas': 'Barajas',
            'carabanchel': 'Carabanchel',
            'usera': 'Usera',
            'latina': 'Latina',
            'moncloa': 'Moncloa-Aravaca',
            'chamberí': 'Chamberí',
            'tetuán': 'Tetuán',
            'salamanca': 'Salamanca',
            'arganzuela': 'Arganzuela',
            'fuencarral': 'Fuencarral-El Pardo',
            'hortaleza': 'Hortaleza',
            'ciudad lineal': 'Ciudad Lineal',
            'moratalaz': 'Moratalaz',
            'vicálvaro': 'Vicálvaro',
            'san blas': 'San Blas-Canillejas',
            'canillejas': 'San Blas-Canillejas',
            'villa de vallecas': 'Villa de Vallecas'
        }
        
        for keyword, district in keywords_districts.items():
            if keyword in station_name_lower:
                return district
        
        return None
    
    def get_air_quality_data_madrid(self, district: str, year: int = 2023) -> List[Dict]:
        """
        Obtener datos de calidad del aire para un distrito específico
        Intenta usar API del Ayuntamiento primero, luego CSV/portal, y finalmente datos simulados
        
        Args:
            district: Nombre del distrito
            year: Año para el que se obtienen los datos (default: 2023)
            
        Returns:
            Lista de diccionarios con datos de calidad del aire
        """
        if district not in self.air_stations:
            logger.error(f"Distrito {district} no encontrado")
            return []
        
        try:
            logger.info(f"Obteniendo datos de calidad del aire para {district} del año {year}")
            
            # 1. Intentar obtener datos de la API del Ayuntamiento de Madrid (fuente principal)
            try:
                logger.info("Intentando obtener datos desde API del Ayuntamiento de Madrid")
                response = requests.get(self.madrid_air_realtime_api, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    # Procesar formato de respuesta
                    if '@graph' in data:
                        stations_data = data['@graph']
                    elif isinstance(data, list):
                        stations_data = data
                    else:
                        stations_data = []
                    
                    # Buscar datos para el distrito específico
                    for station in stations_data:
                        station_id = station.get('id', '').split('/')[-1] if 'id' in station else station.get('codigo', '')
                        district_from_station = self.station_to_district.get(station_id, None)
                        
                        if not district_from_station:
                            station_name = station.get('title', '') or station.get('nombre', '')
                            district_from_station = self._extract_district_from_station_name(station_name)
                        
                        if district_from_station == district:
                            # Extraer valores de contaminantes
                            pollutants = {}
                            for pollutant in ['NO2', 'PM10', 'PM2.5', 'O3', 'SO2', 'CO']:
                                value = station.get(pollutant, station.get(pollutant.lower(), station.get(f'valor_{pollutant.lower()}', None)))
                                if value is not None:
                                    try:
                                        pollutants[pollutant] = float(value)
                                    except (ValueError, TypeError):
                                        pollutants[pollutant] = None
                            
                            if pollutants and any(v is not None for v in pollutants.values()):
                                ica = self._calculate_air_quality_index({k: v for k, v in pollutants.items() if v is not None})
                                result = [{
                                    'fecha': datetime.now().strftime('%Y-%m-%d'),
                                    'distrito': district,
                                    'NO2': pollutants.get('NO2'),
                                    'PM10': pollutants.get('PM10'),
                                    'PM2.5': pollutants.get('PM2.5'),
                                    'O3': pollutants.get('O3'),
                                    'SO2': pollutants.get('SO2'),
                                    'CO': pollutants.get('CO'),
                                    'ICA': ica.get('index', None),
                                    'calidad': ica.get('quality', 'Desconocida'),
                                    'recomendacion': ica.get('recommendation', '')
                                }]
                                logger.info(f"✅ Datos de API obtenidos para {district}")
                                return result
            except Exception as e:
                logger.debug(f"No se pudieron obtener datos de la API: {str(e)}")
            
            # 2. Intentar obtener datos CSV del portal del Ayuntamiento
            csv_data = self.get_air_quality_from_csv()
            if not csv_data.empty and district in csv_data['distrito'].values:
                district_data = csv_data[csv_data['distrito'] == district].copy()
                if not district_data.empty:
                    logger.info(f"✅ Datos CSV obtenidos para {district}: {len(district_data)} registros")
                    return district_data.to_dict('records')
            
            # 3. Generar datos basados en patrones reales como fallback
            logger.info(f"⚠️ Generando datos simulados para {district} basados en patrones reales")
            return self._generate_historical_data(district, year)
            
        except Exception as e:
            logger.error(f"Error al obtener datos de calidad del aire para {district}: {str(e)}")
            return []
    
    def _generate_historical_data(self, district: str, year: int = 2023) -> List[Dict]:
        """
        Generar datos históricos sintéticos basados en patrones realistas para Madrid.
        Usa:
        - Medias de referencia aproximadas para Madrid (self.real_reference_ranges)
        - Factores estacionales (invierno/verano)
        - Patrones de tráfico (laborable/fin de semana)
        - Factor de distrito (tráfico, densidad, etc.)
        
        Args:
            district: Nombre del distrito
            year: Año para el que se generan los datos
            
        Returns:
            Lista de diccionarios con datos de calidad del aire
        """
        air_data = []
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
        current_date = start_date
        
        district_info = self.air_stations[district]
        base_factor = district_info["district_factor"]
        
        # Valores base: media de referencia de Madrid para cada contaminante
        base_values = {p: vals["mean"] for p, vals in self.real_reference_ranges.items()}
        
        # Semilla para reproducibilidad
        np.random.seed(hash(f"{district}_{year}") % (2**32))
        
        while current_date <= end_date:
            month = current_date.month
            weekday = current_date.weekday()
            day_of_year = current_date.timetuple().tm_yday
            
            # Factores estacionales
            if month in [12, 1, 2]:  # Invierno: más NO2/PM, menos O3
                seasonal_factor_no2 = 1.3
                seasonal_factor_pm = 1.3
                seasonal_factor_o3 = 0.7
                seasonal_factor_so2 = 1.2
                seasonal_factor_co = 1.2
            elif month in [6, 7, 8]:  # Verano: más O3, algo menos NO2/PM
                seasonal_factor_no2 = 0.8
                seasonal_factor_pm = 0.8
                seasonal_factor_o3 = 1.5
                seasonal_factor_so2 = 0.9
                seasonal_factor_co = 0.9
            else:  # Primavera/Otoño
                seasonal_factor_no2 = 1.0
                seasonal_factor_pm = 1.0
                seasonal_factor_o3 = 1.0
                seasonal_factor_so2 = 1.0
                seasonal_factor_co = 1.0
            
            # Tráfico según día de la semana
            if weekday < 5:        # Lunes–viernes
                weekday_factor_traffic = 1.15
            elif weekday == 5:     # Sábado
                weekday_factor_traffic = 0.95
            else:                  # Domingo
                weekday_factor_traffic = 0.8
            
            # Variación lenta a lo largo del año + ruido
            seasonal_wave = 0.05 * np.sin(2 * np.pi * day_of_year / 365)
            noise = np.random.normal(0, 0.03)
            daily_variation = 1.0 + seasonal_wave + noise
            
            pollutants = {}
            
            # NO2: muy ligado a tráfico
            no2_raw = (
                base_values["NO2"]
                * base_factor
                * seasonal_factor_no2
                * weekday_factor_traffic
                * daily_variation
                * np.random.lognormal(0, 0.12)
            )
            pollutants["NO2"] = no2_raw
            
            # PM10 y PM2.5: correlacionados
            pm10_raw = (
                base_values["PM10"]
                * base_factor
                * seasonal_factor_pm
                * weekday_factor_traffic
                * daily_variation
                * np.random.lognormal(0, 0.15)
            )
            pollutants["PM10"] = pm10_raw
            pm25_ratio = 0.6 * np.random.uniform(0.9, 1.1)  # 0.54–0.66
            pollutants["PM2.5"] = pm10_raw * pm25_ratio
            
            # O3: inversamente relacionado con NO2
            o3_base = base_values["O3"] * seasonal_factor_o3
            o3_adjustment = max(0.5, 1.0 - (pollutants["NO2"] / 100.0) * 0.3)
            pollutants["O3"] = (
                o3_base
                * o3_adjustment
                * daily_variation
                * np.random.lognormal(0, 0.18)
            )
            
            # SO2: bajo, poca variabilidad, algo más en invierno
            pollutants["SO2"] = (
                base_values["SO2"]
                * seasonal_factor_so2
                * base_factor
                * np.random.lognormal(0, 0.25)
            )
            
            # CO: ligado a tráfico
            pollutants["CO"] = (
                base_values["CO"]
                * base_factor
                * seasonal_factor_co
                * weekday_factor_traffic
                * np.random.lognormal(0, 0.10)
            )
            
            # Normalizar: no negativos y cap a 1.5 × máximo de referencia
            for pol, val in pollutants.items():
                val = max(0.0, float(val))
                max_ref = self.real_reference_ranges[pol]["max"]
                val_capped = min(val, max_ref * 1.5)
                pollutants[pol] = round(val_capped, 2)
            
            # Calcular ICA
            ica = self._calculate_air_quality_index(pollutants)
            
            air_data.append({
                "fecha": current_date.strftime("%Y-%m-%d"),
                "distrito": district,
                "NO2": pollutants["NO2"],
                "PM10": pollutants["PM10"],
                "PM2.5": pollutants["PM2.5"],
                "O3": pollutants["O3"],
                "SO2": pollutants["SO2"],
                "CO": pollutants["CO"],
                "ICA": ica["index"],
                "calidad": ica["quality"],
                "recomendacion": ica["recommendation"],
            })
            
            current_date += timedelta(days=1)
        
        return air_data
    
    def _calculate_air_quality_index(self, pollutants: Dict) -> Dict:
        """
        Calcular índice de calidad del aire basado en los contaminantes
        
        Args:
            pollutants: Diccionario con valores de contaminantes
            
        Returns:
            Diccionario con índice, calidad y recomendación
        """
        # Calcular sub-índices para cada contaminante
        sub_indices = {}
        
        for pollutant, value in pollutants.items():
            limits = self.air_quality_limits[pollutant]
            
            if value <= limits['bueno']:
                sub_indices[pollutant] = 1
            elif value <= limits['moderado']:
                sub_indices[pollutant] = 2
            elif value <= limits['malo']:
                sub_indices[pollutant] = 3
            elif value <= limits['muy_malo']:
                sub_indices[pollutant] = 4
            else:
                sub_indices[pollutant] = 5
        
        # ICA es el máximo de los sub-índices
        ica_index = max(sub_indices.values())
        
        # Clasificación de calidad
        quality_levels = {
            1: "Buena",
            2: "Moderada", 
            3: "Mala",
            4: "Muy mala",
            5: "Extremadamente mala"
        }
        
        # Recomendaciones
        recommendations = {
            1: "Calidad del aire satisfactoria",
            2: "Calidad del aire aceptable para la mayoría de personas",
            3: "Sensibles al aire pueden experimentar síntomas",
            4: "Toda la población puede experimentar efectos",
            5: "Toda la población experimentará efectos graves"
        }
        
        return {
            'index': ica_index,
            'quality': quality_levels[ica_index],
            'recommendation': recommendations[ica_index]
        }
    
    def get_air_quality_all_districts(self, year: int = 2023) -> pd.DataFrame:
        """
        Obtener datos de calidad del aire para todos los distritos del año especificado
        Usa la lógica unificada de get_air_quality_data_madrid() para cada distrito
        
        Args:
            year: Año para el que se obtienen los datos (default: 2023)
            
        Returns:
            DataFrame con datos de calidad del aire
        """
        try:
            logger.info(f"🌍 Obteniendo datos de calidad del aire para todos los distritos del año {year}")
            all_data = []
            
            for district in self.air_stations.keys():
                logger.info(f"Procesando {district}...")
                district_data = self.get_air_quality_data_madrid(district, year)
                all_data.extend(district_data)
                time.sleep(0.1)  # Rate limiting entre distritos
            
            if all_data:
                df = pd.DataFrame(all_data)
                df['fecha'] = pd.to_datetime(df['fecha'])
                df = df.drop_duplicates(subset=['fecha', 'distrito'], keep='last')
                logger.info(f"✅ Datos obtenidos: {len(df)} registros para {len(df['distrito'].unique())} distritos")
                return df
            else:
                logger.warning("No se pudieron obtener datos de calidad del aire")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error al obtener datos de calidad del aire: {str(e)}")
            return pd.DataFrame()
    
    def calculate_air_quality_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calcular métricas de calidad del aire por distrito
        
        Args:
            df: DataFrame con datos de calidad del aire
            
        Returns:
            DataFrame con métricas agregadas
        """
        if df.empty:
            return pd.DataFrame()
        
        # Métricas por contaminante
        pollutant_cols = ['NO2', 'PM10', 'PM2.5', 'O3', 'SO2', 'CO']
        
        metrics = df.groupby('distrito')[pollutant_cols + ['ICA']].agg(['mean', 'std', 'max']).round(2)
        
        # Aplanar columnas
        metrics.columns = ['_'.join(col).strip() for col in metrics.columns]
        metrics = metrics.reset_index()
        
        # Calcular días con calidad buena/mala
        quality_days = df.groupby(['distrito', 'calidad']).size().unstack(fill_value=0)
        quality_days.columns = [f'dias_{col.lower()}' for col in quality_days.columns]
        quality_days = quality_days.reset_index()
        
        # Unir métricas
        metrics = metrics.merge(quality_days, on='distrito', how='left')
        
        # Calcular porcentaje de días con calidad buena
        if 'dias_buena' in metrics.columns and 'dias_moderada' in metrics.columns:
            total_days = metrics['dias_buena'] + metrics['dias_moderada'] + metrics.get('dias_mala', 0)
            metrics['porcentaje_dias_buena'] = (metrics['dias_buena'] / total_days * 100).round(1)
        
        return metrics
    
    def get_health_impact_assessment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Evaluar impacto en la salud por distrito
        
        Args:
            df: DataFrame con datos de calidad del aire
            
        Returns:
            DataFrame con evaluación de impacto en salud
        """
        if df.empty:
            return pd.DataFrame()
        
        health_impact = []
        
        for district in df['distrito'].unique():
            district_data = df[df['distrito'] == district]
            
            # Calcular días con niveles peligrosos
            dangerous_days = len(district_data[district_data['ICA'] >= 3])
            total_days = len(district_data)
            
            # Calcular exposición promedio
            avg_exposure = {
                'NO2': district_data['NO2'].mean(),
                'PM2.5': district_data['PM2.5'].mean(),
                'O3': district_data['O3'].mean()
            }
            
            # Evaluar riesgo
            risk_level = "Bajo"
            if dangerous_days > total_days * 0.3:
                risk_level = "Alto"
            elif dangerous_days > total_days * 0.1:
                risk_level = "Moderado"
            
            health_impact.append({
                'distrito': district,
                'dias_peligrosos': dangerous_days,
                'porcentaje_dias_peligrosos': round(dangerous_days / total_days * 100, 1),
                'exposicion_NO2_promedio': round(avg_exposure['NO2'], 2),
                'exposicion_PM2.5_promedio': round(avg_exposure['PM2.5'], 2),
                'exposicion_O3_promedio': round(avg_exposure['O3'], 2),
                'nivel_riesgo': risk_level
            })
        
        return pd.DataFrame(health_impact)
    
    def save_air_quality_data(self, df: pd.DataFrame, filename: str = "air_quality_madrid.csv"):
        """
        Guardar datos de calidad del aire en archivo CSV
        
        Args:
            df: DataFrame con datos de calidad del aire
            filename: Nombre del archivo
        """
        from utils.file_utils import save_dataframe_to_csv
        try:
            save_dataframe_to_csv(df, filename, subdirectory="processed")
        except ValueError:
            logger.warning("No hay datos para guardar")
    
    def validate_against_reference(self, df: pd.DataFrame) -> Dict:
        """
        Validar que los datos generados están dentro de rangos razonables de referencia
        
        Args:
            df: DataFrame con datos de calidad del aire
            
        Returns:
            Diccionario con estadísticas de validación
        """
        if df.empty:
            return {"error": "DataFrame vacío"}
        
        validation_results = {}
        pollutant_cols = ['NO2', 'PM10', 'PM2.5', 'O3', 'SO2', 'CO']
        
        for pollutant in pollutant_cols:
            if pollutant not in df.columns:
                continue
                
            ref_range = self.real_reference_ranges[pollutant]
            values = df[pollutant].dropna()
            
            if len(values) == 0:
                continue
            
            mean_val = float(values.mean())
            min_val = float(values.min())
            max_val = float(values.max())
            
            # Verificar si está dentro de rangos razonables (con cierto margen)
            ref_mean = ref_range['mean']
            ref_min = ref_range['min']
            ref_max = ref_range['max']
            
            # Permitir variación del ±50% de la media de referencia
            mean_ok = abs(mean_val - ref_mean) / ref_mean <= 0.5 if ref_mean > 0 else True
            min_ok = min_val >= ref_min * 0.5  # Permitir valores hasta 50% más bajos
            max_ok = max_val <= ref_max * 1.5  # Permitir valores hasta 50% más altos
            
            validation_results[pollutant] = {
                'mean': round(mean_val, 2),
                'ref_mean': ref_mean,
                'mean_ok': mean_ok,
                'min': round(min_val, 2),
                'ref_min': ref_min,
                'min_ok': min_ok,
                'max': round(max_val, 2),
                'ref_max': ref_max,
                'max_ok': max_ok,
                'valid': mean_ok and min_ok and max_ok
            }
        
        # Resumen general
        all_valid = all(v.get('valid', False) for v in validation_results.values())
        validation_results['summary'] = {
            'all_valid': all_valid,
            'pollutants_checked': len(validation_results) - 1,  # Excluir 'summary'
            'within_ranges': sum(1 for v in validation_results.values() if isinstance(v, dict) and v.get('valid', False))
        }
        
        return validation_results

def main():
    """
    Función principal para ejecutar la recopilación de datos de calidad del aire
        Usa API del Ayuntamiento de Madrid como fuente principal, con fallbacks a CSV/portal y simulación
    """
    # Crear instancia del colector
    collector = AirQualityCollector()
    
    print("🌍 Iniciando recopilación de datos de calidad del aire para Madrid")
    print("📡 Fuentes de datos: API Ayuntamiento Madrid (principal), CSV/Portal (alternativa), Simulación (fallback)")
    
    # Obtener datos para todos los distritos del año 2023
    air_quality_df = collector.get_air_quality_all_districts(year=2023)
    
    if not air_quality_df.empty:
        print(f"\n📊 Datos obtenidos:")
        print(f"   - Total de registros: {len(air_quality_df)}")
        print(f"   - Distritos cubiertos: {len(air_quality_df['distrito'].unique())}")
        print(f"   - Rango de fechas: {air_quality_df['fecha'].min()} a {air_quality_df['fecha'].max()}")
        
        # Validar datos contra rangos de referencia
        print("\n🔍 Validando datos contra rangos de referencia...")
        validation = collector.validate_against_reference(air_quality_df)
        if 'summary' in validation:
            summary = validation['summary']
            print(f"   - Contaminantes verificados: {summary['pollutants_checked']}")
            print(f"   - Dentro de rangos razonables: {summary['within_ranges']}/{summary['pollutants_checked']}")
            if summary['all_valid']:
                print("   ✅ Todos los contaminantes están dentro de rangos razonables")
            else:
                print("   ⚠️ Algunos contaminantes están fuera de los rangos de referencia")
        
        # Calcular métricas
        print("\n🧮 Calculando métricas...")
        metrics_df = collector.calculate_air_quality_metrics(air_quality_df)
        health_df = collector.get_health_impact_assessment(air_quality_df)
        
        # Guardar datos
        print("\n💾 Guardando datos...")
        collector.save_air_quality_data(air_quality_df, "air_quality_madrid.csv")
        collector.save_air_quality_data(metrics_df, "air_quality_metrics_madrid.csv")
        collector.save_air_quality_data(health_df, "air_quality_health_madrid.csv")
        
        print(f"\n✅ Proceso completado:")
        print(f"   - Datos de calidad del aire: {len(air_quality_df)} registros")
        print(f"   - Métricas calculadas: {len(metrics_df)} distritos")
        print(f"   - Evaluación de salud: {len(health_df)} distritos")
        print(f"   - Archivos guardados en: data/processed/")
    else:
        print("❌ No se pudieron recopilar datos de calidad del aire")
        print("   Verifique la conexión a internet y las APIs disponibles")

if __name__ == "__main__":
    main()
