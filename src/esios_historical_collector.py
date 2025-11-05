import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from esios_api import ESIOSAPI
import pandas as pd
from datetime import datetime, timedelta
import time
import matplotlib.pyplot as plt

class ESIOSHistoricalCollector:
    """
    Colector de datos históricos de e·sios para análisis temporal y ML
    """
    
    def __init__(self, api_key: str):
        self.esios = ESIOSAPI(api_key)
        self.api_key = api_key
        
    def get_historical_data(self, indicator_id: int, days_back: int = 30) -> pd.DataFrame:
        """
        Obtiene datos históricos de un indicador específico
        """
        print(f"Obteniendo datos históricos del indicador {indicator_id} (últimos {days_back} días)...")
        
        all_data = []
        
        # Obtener datos día por día para evitar limitaciones de la API
        for i in range(days_back):
            target_date = datetime.now() - timedelta(days=i)
            date_str = target_date.strftime('%Y-%m-%d')
            
            try:
                # Obtener información del indicador para esa fecha
                response = self.esios._make_request(f"indicators/{indicator_id}")
                
                if response and 'indicator' in response:
                    indicator = response['indicator']
                    values = indicator.get('values', [])
                    
                    # Filtrar valores de la fecha específica
                    date_values = [v for v in values if v.get('datetime', '').startswith(date_str)]
                    
                    for value in date_values:
                        all_data.append({
                            'indicator_id': indicator_id,
                            'name': indicator.get('name', ''),
                            'unit': indicator.get('unit', ''),
                            'datetime': value.get('datetime', ''),
                            'value': value.get('value', ''),
                            'geo_id': value.get('geo_id', ''),
                            'geo_name': value.get('geo_name', ''),
                            'geo_agg': value.get('geo_agg', '')
                        })
                
                # Respetar rate limiting
                time.sleep(1)
                
            except Exception as e:
                print(f"Error obteniendo datos para {date_str}: {e}")
                continue
        
        if all_data:
            df = pd.DataFrame(all_data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime')
            print(f"✅ Obtenidos {len(df)} registros históricos")
            return df
        else:
            print(f"❌ No se obtuvieron datos históricos")
            return pd.DataFrame()
    
    def collect_historical_dataset(self, days_back: int = 30):
        """
        Recolecta un dataset histórico completo para ML
        """
        print(f"🚀 RECOLECTANDO DATASET HISTÓRICO ({days_back} días)")
        print("="*60)
        
        # Usar rutas centralizadas (los directorios ya se crean automáticamente)
        from config.paths import DATA_ESIOS_HISTORICAL, DATA_ESIOS_PROCESSED_HISTORICAL
        
        # Indicadores clave para ML
        key_indicators = [
            {'id': 1293, 'name': 'Demanda real', 'type': 'demand'},
            {'id': 1001, 'name': 'Precio PVPC 2.0TD', 'type': 'price'},
            {'id': 1, 'name': 'Generación Hidráulica UGH', 'type': 'generation'},
            {'id': 4, 'name': 'Generación Nuclear', 'type': 'generation'},
            {'id': 9, 'name': 'Generación Ciclo Combinado', 'type': 'generation'},
            {'id': 12, 'name': 'Generación Eólica', 'type': 'generation'}
        ]
        
        all_historical_data = {}
        
        for indicator in key_indicators:
            print(f"\n📊 Recolectando: {indicator['name']} (ID: {indicator['id']})")
            
            df = self.get_historical_data(indicator['id'], days_back)
            
            if not df.empty:
                # Guardar datos individuales usando rutas centralizadas
                from config.paths import DATA_ESIOS_HISTORICAL
                filename = DATA_ESIOS_HISTORICAL / f"{indicator['type']}_{indicator['id']}.csv"
                df.to_csv(filename, index=False, encoding='utf-8')
                print(f"💾 Guardado en: {filename}")
                
                all_historical_data[indicator['id']] = {
                    'name': indicator['name'],
                    'type': indicator['type'],
                    'data': df
                }
        
        # Crear dataset consolidado para ML
        self.create_ml_dataset(all_historical_data)
        
        return all_historical_data
    
    def create_ml_dataset(self, historical_data: dict):
        """
        Crea un dataset consolidado para machine learning
        """
        print("\n🤖 CREANDO DATASET PARA MACHINE LEARNING")
        print("="*50)
        
        if not historical_data:
            print("❌ No hay datos históricos para procesar")
            return
        
        # Encontrar el rango temporal común
        all_dates = set()
        for indicator_id, data in historical_data.items():
            df = data['data']
            if not df.empty:
                all_dates.update(df['datetime'].dt.date)
        
        if not all_dates:
            print("❌ No se encontraron fechas comunes")
            return
        
        # Crear índice temporal
        min_date = min(all_dates)
        max_date = max(all_dates)
        
        print(f"📅 Rango temporal: {min_date} a {max_date}")
        
        # Crear dataset consolidado
        ml_data = []
        
        for date in sorted(all_dates):
            date_data = {'date': date}
            
            # Agregar datos de cada indicador para esta fecha
            for indicator_id, data in historical_data.items():
                df = data['data']
                if not df.empty:
                    date_df = df[df['datetime'].dt.date == date]
                    
                    if not date_df.empty:
                        # Calcular estadísticas diarias
                        date_data[f"{data['type']}_{indicator_id}_mean"] = date_df['value'].mean()
                        date_data[f"{data['type']}_{indicator_id}_max"] = date_df['value'].max()
                        date_data[f"{data['type']}_{indicator_id}_min"] = date_df['value'].min()
                        date_data[f"{data['type']}_{indicator_id}_std"] = date_df['value'].std()
                        
                        # Agregar datos por hora (si hay suficientes datos)
                        for hour in range(24):
                            hour_data = date_df[date_df['datetime'].dt.hour == hour]
                            if not hour_data.empty:
                                date_data[f"{data['type']}_{indicator_id}_h{hour:02d}"] = hour_data['value'].mean()
            
            ml_data.append(date_data)
        
        # Crear DataFrame
        ml_df = pd.DataFrame(ml_data)
        ml_df['date'] = pd.to_datetime(ml_df['date'])
        
        # Agregar características temporales
        ml_df['year'] = ml_df['date'].dt.year
        ml_df['month'] = ml_df['date'].dt.month
        ml_df['day'] = ml_df['date'].dt.day
        ml_df['dayofweek'] = ml_df['date'].dt.dayofweek
        ml_df['is_weekend'] = ml_df['dayofweek'].isin([5, 6]).astype(int)
        ml_df['quarter'] = ml_df['date'].dt.quarter
        
        # Guardar dataset para ML usando rutas centralizadas
        from config.paths import DATA_ESIOS_PROCESSED_HISTORICAL
        ml_filename = DATA_ESIOS_PROCESSED_HISTORICAL / 'ml_dataset.csv'
        ml_df.to_csv(ml_filename, index=False, encoding='utf-8')
        
        print(f"✅ Dataset ML creado: {len(ml_df)} filas, {len(ml_df.columns)} columnas")
        print(f"💾 Guardado en: {ml_filename}")
        
        # Mostrar resumen
        print(f"\n📊 RESUMEN DEL DATASET:")
        print(f"   Período: {ml_df['date'].min().date()} a {ml_df['date'].max().date()}")
        print(f"   Días: {len(ml_df)}")
        print(f"   Variables: {len(ml_df.columns) - 1}")  # -1 por la columna date
        
        # Mostrar columnas disponibles
        print(f"\n🔍 VARIABLES DISPONIBLES:")
        for col in sorted(ml_df.columns):
            if col != 'date':
                print(f"   - {col}")
        
        return ml_df

def create_individual_visualizations():
    """
    Crea visualizaciones unitarias más claras
    """
    print("\n📊 CREANDO VISUALIZACIONES UNITARIAS")
    print("="*50)
    
    # Configurar estilo
    plt.style.use('seaborn-v0_8')
    
    # 1. Demanda real - Evolución temporal
    try:
        from config.paths import DATA_ESIOS
        df = pd.read_csv(DATA_ESIOS / 'demand_data.csv')
        demand_real = df[df['indicator_id'] == 1293]
        
        if not demand_real.empty:
            demand_real['datetime'] = pd.to_datetime(demand_real['datetime'])
            demand_real = demand_real.sort_values('datetime')
            
            plt.figure(figsize=(12, 6))
            plt.plot(demand_real['datetime'], demand_real['value'], linewidth=2, color='blue')
            plt.title('Demanda Real Eléctrica - Evolución Temporal', fontsize=16, fontweight='bold')
            plt.xlabel('Fecha y Hora', fontsize=12)
            plt.ylabel('Demanda (MW)', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            from config.paths import DATA_ESIOS_PROCESSED
            plt.savefig(DATA_ESIOS_PROCESSED / 'demand_temporal.png', dpi=300, bbox_inches='tight')
            plt.show()
            print("✅ Demanda temporal guardada")
    except Exception as e:
        print(f"❌ Error creando visualización de demanda: {e}")
    
    # 2. Precios PVPC - Evolución temporal
    try:
        from config.paths import DATA_ESIOS
        df = pd.read_csv(DATA_ESIOS / 'price_data.csv')
        price_pvpc = df[df['indicator_id'] == 1001]
        price_pvpc = price_pvpc[price_pvpc['geo_id'] == 8741]  # Solo Península
        
        if not price_pvpc.empty:
            price_pvpc['datetime'] = pd.to_datetime(price_pvpc['datetime'])
            price_pvpc = price_pvpc.sort_values('datetime')
            
            plt.figure(figsize=(12, 6))
            plt.plot(price_pvpc['datetime'], price_pvpc['value'], linewidth=2, color='red')
            plt.title('Precios PVPC 2.0TD - Evolución Temporal (Península)', fontsize=16, fontweight='bold')
            plt.xlabel('Fecha y Hora', fontsize=12)
            plt.ylabel('Precio (€/MWh)', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            from config.paths import DATA_ESIOS_PROCESSED
            plt.savefig(DATA_ESIOS_PROCESSED / 'price_temporal.png', dpi=300, bbox_inches='tight')
            plt.show()
            print("✅ Precios temporales guardados")
    except Exception as e:
        print(f"❌ Error creando visualización de precios: {e}")
    
    # 3. Generación por tipo - Comparación
    try:
        from config.paths import DATA_ESIOS
        df = pd.read_csv(DATA_ESIOS / 'generation_data.csv')
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Agrupar por tipo de generación
        generation_summary = df.groupby(['indicator_id', 'indicator_name'])['value'].mean().reset_index()
        
        plt.figure(figsize=(12, 8))
        bars = plt.barh(range(len(generation_summary)), generation_summary['value'])
        plt.yticks(range(len(generation_summary)), 
                  [f"{row['indicator_name'][:30]}..." for _, row in generation_summary.iterrows()])
        plt.title('Generación Eléctrica Promedio por Tipo', fontsize=16, fontweight='bold')
        plt.xlabel('Generación Promedio (MW)', fontsize=12)
        plt.grid(True, alpha=0.3)
        
        # Agregar valores en las barras
        for i, bar in enumerate(bars):
            width = bar.get_width()
            plt.text(width + 50, bar.get_y() + bar.get_height()/2, 
                    f'{width:.0f} MW', ha='left', va='center')
        
        plt.tight_layout()
        from config.paths import DATA_ESIOS_PROCESSED
        plt.savefig(DATA_ESIOS_PROCESSED / 'generation_comparison.png', dpi=300, bbox_inches='tight')
        plt.show()
        print("✅ Comparación de generación guardada")
    except Exception as e:
        print(f"❌ Error creando visualización de generación: {e}")

def main():
    """
    Función principal
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        print("   Por favor, configura ESIOS_API_KEY como variable de entorno")
        return
    
    collector = ESIOSHistoricalCollector(API_KEY)
    
    # Recolectar datos históricos (empezar con 7 días para probar)
    print("⚠️  NOTA: Recolectando datos históricos limitados para evitar sobrecarga de la API")
    historical_data = collector.collect_historical_dataset(days_back=7)
    
    # Crear visualizaciones unitarias
    create_individual_visualizations()
    
    print("\n✅ PROCESO COMPLETADO")
    print("Archivos generados:")
    print("- data/processed/esios/historical/ml_dataset.csv")
    print("- data/processed/esios/*_temporal.png")
    print("- data/processed/esios/generation_comparison.png")

if __name__ == "__main__":
    main()
