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
# Nota: Los colectores individuales (air_quality, mobility, weather, electricity_prices)
# son usados internamente por data_integration, no se ejecutan directamente aquí
try:
    from data_integration import DataIntegration
    DATA_INTEGRATION_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar data_integration: {e}")
    DATA_INTEGRATION_AVAILABLE = False

try:
    from ine_api import load_renta_media, load_indicadores_demograficos, load_nivel_educativo
    from ine_api_electric import load_consumo_madrid_distritos
    INE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar módulos INE: {e}")
    INE_AVAILABLE = False

try:
    from esios_data_collector import ESIOSDataCollector
    from config.settings import get_api_key
    ESIOS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"No se pudo importar módulos ESIOS: {e}")
    ESIOS_AVAILABLE = False

import os


class DataCollectionRunner:
    """Clase para ejecutar todos los colectores de datos"""
    
    def __init__(self):
        self.results = {}
        self.start_time = datetime.now()
        self.data_dir = Path(__file__).parent / "data" / "processed"
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
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
    
    def run_ine_collection(self):
        """Ejecutar recopilación de datos del INE (Instituto Nacional de Estadística)"""
        if not INE_AVAILABLE:
            logger.warning("⏭️  Colector de datos INE no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("📊 INICIANDO RECOPILACIÓN DE DATOS DEL INE")
            logger.info("=" * 60)
            
            files_generated = []
            
            # 1. Datos de consumo eléctrico por distritos (tabla 59532)
            logger.info("⚡ Recopilando datos de consumo eléctrico del INE...")
            try:
                consumo_df = load_consumo_madrid_distritos(header_row=6, add_distrito_nombre=True)
                if not consumo_df.empty:
                    consumo_path = self.data_dir / "ine_consumo_electrico_madrid.csv"
                    consumo_df.to_csv(consumo_path, index=False, encoding='utf-8')
                    files_generated.append("ine_consumo_electrico_madrid.csv")
                    logger.info(f"✅ Datos de consumo: {len(consumo_df)} registros")
                else:
                    logger.warning("⚠️ No se obtuvieron datos de consumo eléctrico")
            except Exception as e:
                logger.error(f"❌ Error obteniendo datos de consumo: {str(e)}")
            
            # 2. Datos de renta media por distrito (tabla 31097)
            logger.info("💰 Recopilando datos de renta media del INE...")
            try:
                renta_df = load_renta_media()
                if not renta_df.empty:
                    renta_path = self.data_dir / "ine_renta_madrid.csv"
                    renta_df.to_csv(renta_path, index=False, encoding='utf-8')
                    files_generated.append("ine_renta_madrid.csv")
                    logger.info(f"✅ Datos de renta: {len(renta_df)} registros")
                else:
                    logger.warning("⚠️ No se obtuvieron datos de renta")
            except Exception as e:
                logger.error(f"❌ Error obteniendo datos de renta: {str(e)}")
            
            # 3. Indicadores demográficos (tabla 31105)
            logger.info("👥 Recopilando indicadores demográficos del INE...")
            try:
                demo_df = load_indicadores_demograficos()
                if not demo_df.empty:
                    demo_path = self.data_dir / "ine_demografia_madrid.csv"
                    demo_df.to_csv(demo_path, index=False, encoding='utf-8')
                    files_generated.append("ine_demografia_madrid.csv")
                    logger.info(f"✅ Datos demográficos: {len(demo_df)} registros")
                else:
                    logger.warning("⚠️ No se obtuvieron datos demográficos")
            except Exception as e:
                logger.error(f"❌ Error obteniendo datos demográficos: {str(e)}")
            
            # 4. Nivel educativo (tabla 66753)
            logger.info("📚 Recopilando datos de nivel educativo del INE...")
            try:
                educacion_df = load_nivel_educativo()
                if not educacion_df.empty:
                    educ_path = self.data_dir / "ine_educacion_madrid.csv"
                    educacion_df.to_csv(educ_path, index=False, encoding='utf-8')
                    files_generated.append("ine_educacion_madrid.csv")
                    logger.info(f"✅ Datos de educación: {len(educacion_df)} registros")
                else:
                    logger.warning("⚠️ No se obtuvieron datos de educación")
            except Exception as e:
                logger.error(f"❌ Error obteniendo datos de educación: {str(e)}")
            
            if files_generated:
                self.results['ine'] = {
                    'status': 'success',
                    'files': files_generated
                }
                return True
            else:
                logger.error("❌ No se obtuvieron datos del INE")
                self.results['ine'] = {'status': 'failed', 'reason': 'No data obtained'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en recopilación de datos INE: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['ine'] = {'status': 'error', 'error': str(e)}
            return False
    
    def run_esios_collection(self):
        """Ejecutar recopilación de datos de ESIOS (Red Eléctrica de España)"""
        if not ESIOS_AVAILABLE:
            logger.warning("⏭️  Colector de datos ESIOS no disponible")
            return False
            
        try:
            logger.info("=" * 60)
            logger.info("⚡ INICIANDO RECOPILACIÓN DE DATOS DE ESIOS")
            logger.info("=" * 60)
            
            # Obtener API key de ESIOS
            try:
                api_key = get_api_key("ESIOS")
            except ValueError as e:
                logger.error(f"❌ Error de configuración ESIOS: {e}")
                logger.error("   Por favor, configura ESIOS_API_KEY como variable de entorno")
                self.results['esios'] = {'status': 'failed', 'reason': 'API key not configured'}
                return False
            
            collector = ESIOSDataCollector(api_key)
            
            # Recopilar todos los datos de ESIOS
            collector.collect_all_data()
            
            # Verificar que se generaron los archivos
            # DATA_ESIOS ya está disponible porque src está en el path
            from config.paths import DATA_ESIOS
            files_generated = []
            expected_files = [
                'demand_data.csv',
                'price_data.csv',
                'generation_data.csv'
            ]
            
            for filename in expected_files:
                filepath = DATA_ESIOS / filename
                if filepath.exists():
                    files_generated.append(f"esios/{filename}")
                    size_kb = filepath.stat().st_size / 1024
                    logger.info(f"✅ {filename} ({size_kb:.2f} KB)")
                else:
                    logger.warning(f"⚠️ {filename} no se generó")
            
            if files_generated:
                self.results['esios'] = {
                    'status': 'success',
                    'files': files_generated
                }
                return True
            else:
                logger.error("❌ No se generaron archivos de ESIOS")
                self.results['esios'] = {'status': 'failed', 'reason': 'No files generated'}
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en recopilación de datos ESIOS: {str(e)}")
            logger.debug(traceback.format_exc())
            self.results['esios'] = {'status': 'error', 'error': str(e)}
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
        
        # 1. Datos del INE (Instituto Nacional de Estadística)
        total_count += 1
        if self.run_ine_collection():
            success_count += 1
        
        # 2. Datos de ESIOS (Red Eléctrica de España)
        total_count += 1
        if self.run_esios_collection():
            success_count += 1
        
        # 3. Integración de datos (weather, air_quality, mobility, electricity_prices)
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
        choices=['ine', 'esios', 'data_integration'],
        help='Ejecutar solo los colectores especificados (ine, esios, data_integration)'
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

