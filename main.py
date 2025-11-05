"""
Script principal para ejecutar todos los colectores de datos que generan archivos CSV.

Este script ejecuta secuencialmente todos los scripts Python relevantes que
obtienen y procesan datos y los guardan en formato CSV en el directorio data/processed/.
"""

import sys
import traceback
from pathlib import Path
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_collection.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# Añadir src al path para imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Importar los colectores
try:
    from air_quality_collector import AirQualityCollector
    AIR_QUALITY_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar air_quality_collector: {e}")
    AIR_QUALITY_AVAILABLE = False

try:
    from mobility_data_collector import MobilityDataCollector
    MOBILITY_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar mobility_data_collector: {e}")
    MOBILITY_AVAILABLE = False

try:
    from weather_data_collector import WeatherDataCollector
    WEATHER_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar weather_data_collector: {e}")
    WEATHER_AVAILABLE = False

try:
    from electricity_prices_collector import ElectricityPricesCollector
    ELECTRICITY_PRICES_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar electricity_prices_collector: {e}")
    ELECTRICITY_PRICES_AVAILABLE = False

try:
    from data_integration import DataIntegration
    DATA_INTEGRATION_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar data_integration: {e}")
    DATA_INTEGRATION_AVAILABLE = False

import os


class DataCollectionRunner:
    """Clase para ejecutar todos los colectores de datos"""
    
    def __init__(self):
        self.results = {}
        self.start_time = datetime.now()
        self.data_dir = Path(__file__).parent / "data" / "processed"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
    def run_air_quality_collection(self):
        """Ejecutar recopilación de datos de calidad del aire"""
        if not AIR_QUALITY_AVAILABLE:
            logger.warning("⏭️  Colector de calidad del aire no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("🌬️  INICIANDO RECOPILACIÓN DE CALIDAD DEL AIRE")
            logger.info("=" * 60)
            
            collector = AirQualityCollector()
            
            # Obtener datos para todos los distritos
            air_quality_df = collector.get_air_quality_all_districts(year=2022)
            
            if not air_quality_df.empty:
                logger.info(f"✅ Datos obtenidos: {len(air_quality_df)} registros")
                
                # Calcular métricas
                metrics_df = collector.calculate_air_quality_metrics(air_quality_df)
                health_df = collector.get_health_impact_assessment(air_quality_df)
                
                # Guardar datos
                collector.save_air_quality_data(air_quality_df, "air_quality_madrid.csv")
                collector.save_air_quality_data(metrics_df, "air_quality_metrics_madrid.csv")
                collector.save_air_quality_data(health_df, "air_quality_health_madrid.csv")
                
                self.results['air_quality'] = {
                    'status': 'success',
                    'records': len(air_quality_df),
                    'districts': len(air_quality_df['distrito'].unique()) if 'distrito' in air_quality_df.columns else 0,
                    'files': ['air_quality_madrid.csv', 'air_quality_metrics_madrid.csv', 'air_quality_health_madrid.csv']
                }
                return True
            else:
                logger.error("❌ No se obtuvieron datos de calidad del aire")
                self.results['air_quality'] = {'status': 'failed', 'reason': 'No data obtained'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en recopilación de calidad del aire: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['air_quality'] = {'status': 'error', 'error': str(e)}
            return False
    
    def run_mobility_collection(self):
        """Ejecutar recopilación de datos de movilidad"""
        if not MOBILITY_AVAILABLE:
            logger.warning("⏭️  Colector de movilidad no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("🚇 INICIANDO RECOPILACIÓN DE MOVILIDAD")
            logger.info("=" * 60)
            
            collector = MobilityDataCollector()
            
            # Obtener datos para todos los distritos
            mobility_df = collector.get_mobility_data_all_districts()
            
            if not mobility_df.empty:
                logger.info(f"✅ Datos obtenidos: {len(mobility_df)} registros")
                
                # Calcular métricas
                metrics_df = collector.calculate_mobility_metrics(mobility_df)
                connectivity_df = collector.get_connectivity_analysis(mobility_df)
                
                # Guardar datos
                collector.save_mobility_data(mobility_df, "mobility_data_madrid.csv")
                collector.save_mobility_data(metrics_df, "mobility_metrics_madrid.csv")
                collector.save_mobility_data(connectivity_df, "mobility_connectivity_madrid.csv")
                
                self.results['mobility'] = {
                    'status': 'success',
                    'records': len(mobility_df),
                    'files': ['mobility_data_madrid.csv', 'mobility_metrics_madrid.csv', 'mobility_connectivity_madrid.csv']
                }
                return True
            else:
                logger.error("❌ No se obtuvieron datos de movilidad")
                self.results['mobility'] = {'status': 'failed', 'reason': 'No data obtained'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en recopilación de movilidad: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['mobility'] = {'status': 'error', 'error': str(e)}
            return False
    
    def run_weather_collection(self):
        """Ejecutar recopilación de datos meteorológicos"""
        if not WEATHER_AVAILABLE:
            logger.warning("⏭️  Colector de datos meteorológicos no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("🌤️  INICIANDO RECOPILACIÓN DE DATOS METEOROLÓGICOS")
            logger.info("=" * 60)
            
            api_key = os.getenv('OPENWEATHER_API_KEY')
            if not api_key:
                logger.warning("⚠️  OPENWEATHER_API_KEY no configurada, se usará datos simulados")
            
            collector = WeatherDataCollector(api_key)
            
            # Obtener datos para todos los distritos
            weather_df = collector.get_weather_data_all_districts(days_back=30)
            
            if not weather_df.empty:
                logger.info(f"✅ Datos obtenidos: {len(weather_df)} registros")
                
                # Calcular métricas
                metrics_df = collector.calculate_weather_metrics(weather_df)
                
                # Guardar datos
                collector.save_weather_data(weather_df, "weather_data_madrid.csv")
                collector.save_weather_data(metrics_df, "weather_metrics_madrid.csv")
                
                self.results['weather'] = {
                    'status': 'success',
                    'records': len(weather_df),
                    'files': ['weather_data_madrid.csv', 'weather_metrics_madrid.csv']
                }
                return True
            else:
                logger.error("❌ No se obtuvieron datos meteorológicos")
                self.results['weather'] = {'status': 'failed', 'reason': 'No data obtained'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en recopilación de datos meteorológicos: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['weather'] = {'status': 'error', 'error': str(e)}
            return False
    
    def run_electricity_prices_collection(self):
        """Ejecutar recopilación de datos de precios de electricidad"""
        if not ELECTRICITY_PRICES_AVAILABLE:
            logger.warning("⏭️  Colector de precios de electricidad no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("⚡ INICIANDO RECOPILACIÓN DE PRECIOS DE ELECTRICIDAD")
            logger.info("=" * 60)
            
            collector = ElectricityPricesCollector()
            
            # Obtener precios históricos
            prices_df = collector.get_historical_prices(days_back=30)
            
            if not prices_df.empty:
                logger.info(f"✅ Datos obtenidos: {len(prices_df)} registros")
                
                # Calcular métricas
                metrics_df = collector.calculate_price_metrics(prices_df)
                
                # Guardar datos
                collector.save_price_data(prices_df, "electricity_prices_madrid.csv")
                collector.save_price_data(metrics_df, "electricity_price_metrics_madrid.csv")
                
                self.results['electricity_prices'] = {
                    'status': 'success',
                    'records': len(prices_df),
                    'files': ['electricity_prices_madrid.csv', 'electricity_price_metrics_madrid.csv']
                }
                return True
            else:
                logger.error("❌ No se obtuvieron datos de precios de electricidad")
                self.results['electricity_prices'] = {'status': 'failed', 'reason': 'No data obtained'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en recopilación de precios de electricidad: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['electricity_prices'] = {'status': 'error', 'error': str(e)}
            return False
    
    def run_data_integration(self):
        """Ejecutar integración de datos"""
        if not DATA_INTEGRATION_AVAILABLE:
            logger.warning("⏭️  Integrador de datos no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("🔗 INICIANDO INTEGRACIÓN DE DATOS")
            logger.info("=" * 60)
            
            api_key = os.getenv('OPENWEATHER_API_KEY')
            integrator = DataIntegration(api_key)
            
            # Recopilar todos los datos adicionales
            additional_data = integrator.collect_all_additional_data(days_back=30)
            
            if additional_data:
                logger.info(f"✅ Datos recopilados: {len(additional_data)} tipos")
                
                # Calcular métricas adicionales
                additional_metrics = integrator.calculate_additional_metrics(additional_data)
                
                # Guardar datos
                files_generated = []
                for data_type, df in additional_data.items():
                    filename = f"{data_type}_data_madrid.csv"
                    integrator.weather_collector.save_weather_data(df, filename)
                    files_generated.append(filename)
                
                for data_type, df in additional_metrics.items():
                    filename = f"{data_type}_madrid.csv"
                    integrator.weather_collector.save_weather_data(df, filename)
                    files_generated.append(filename)
                
                self.results['data_integration'] = {
                    'status': 'success',
                    'data_types': list(additional_data.keys()),
                    'files': files_generated
                }
                return True
            else:
                logger.error("❌ No se pudieron recopilar datos adicionales")
                self.results['data_integration'] = {'status': 'failed', 'reason': 'No data obtained'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en integración de datos: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['data_integration'] = {'status': 'error', 'error': str(e)}
            return False
    
    def run_all(self, skip_integration=False):
        """
        Ejecutar todos los colectores de datos
        
        Args:
            skip_integration: Si True, omite la integración de datos (puede ser redundante)
        """
        logger.info("=" * 80)
        logger.info("🚀 INICIANDO RECOPILACIÓN COMPLETA DE DATOS")
        logger.info("=" * 80)
        logger.info(f"📅 Inicio: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📁 Directorio de salida: {self.data_dir}")
        logger.info("")
        
        # Ejecutar todos los colectores
        success_count = 0
        total_count = 0
        
        # 1. Calidad del aire
        total_count += 1
        if self.run_air_quality_collection():
            success_count += 1
        
        # 2. Movilidad
        total_count += 1
        if self.run_mobility_collection():
            success_count += 1
        
        # 3. Datos meteorológicos
        total_count += 1
        if self.run_weather_collection():
            success_count += 1
        
        # 4. Precios de electricidad
        total_count += 1
        if self.run_electricity_prices_collection():
            success_count += 1
        
        # 5. Integración de datos (opcional, puede ser redundante)
        if not skip_integration:
            total_count += 1
            if self.run_data_integration():
                success_count += 1
        
        # Resumen final
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 RESUMEN FINAL")
        logger.info("=" * 80)
        logger.info(f"✅ Colectores ejecutados exitosamente: {success_count}/{total_count}")
        logger.info(f"⏱️  Duración total: {duration:.2f} segundos")
        logger.info("")
        logger.info("📋 Detalles por colector:")
        for name, result in self.results.items():
            status_icon = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"  {status_icon} {name.replace('_', ' ').title()}: {result['status']}")
            if result['status'] == 'success':
                if 'records' in result:
                    logger.info(f"     - Registros: {result['records']}")
                if 'files' in result:
                    logger.info(f"     - Archivos generados: {len(result['files'])}")
                    for file in result['files']:
                        filepath = self.data_dir / file
                        if filepath.exists():
                            size_kb = filepath.stat().st_size / 1024
                            logger.info(f"       • {file} ({size_kb:.2f} KB)")
        logger.info("")
        logger.info(f"📁 Todos los archivos CSV guardados en: {self.data_dir}")
        logger.info("=" * 80)
        
        return success_count == total_count


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Ejecutar todos los colectores de datos CSV')
    parser.add_argument(
        '--skip-integration',
        action='store_true',
        help='Omitir la integración de datos (puede ser redundante con otros colectores)'
    )
    parser.add_argument(
        '--only',
        nargs='+',
        choices=['air_quality', 'mobility', 'weather', 'electricity_prices', 'data_integration'],
        help='Ejecutar solo los colectores especificados'
    )
    
    args = parser.parse_args()
    
    runner = DataCollectionRunner()
    
    if args.only:
        # Ejecutar solo los colectores especificados
        logger.info(f"🎯 Ejecutando solo: {', '.join(args.only)}")
        for collector_name in args.only:
            method_name = f"run_{collector_name}"
            if hasattr(runner, method_name):
                getattr(runner, method_name)()
    else:
        # Ejecutar todos
        runner.run_all(skip_integration=args.skip_integration)


if __name__ == "__main__":
    main()

