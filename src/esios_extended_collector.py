import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from esios_api import ESIOSAPI
import pandas as pd
from datetime import datetime, timedelta
import time
import numpy as np

class ESIOSExtendedCollector:
    """
    Colector extendido de datos históricos de e·sios
    """
    
    def __init__(self, api_key: str):
        self.esios = ESIOSAPI(api_key)
        self.api_key = api_key
        
    def get_extended_historical_data(self, days_back: int = 30):
        """
        Obtiene datos históricos extendidos de manera más eficiente
        """
        print(f"🚀 RECOLECTANDO DATOS HISTÓRICOS EXTENDIDOS ({days_back} días)")
        print("="*70)
        
        # Usar rutas centralizadas (los directorios ya se crean automáticamente)
        from config.paths import DATA_ESIOS_HISTORICAL, DATA_ESIOS_PROCESSED_HISTORICAL
        
        # Indicadores clave con menos restricciones
        key_indicators = [
            {'id': 1293, 'name': 'Demanda real', 'type': 'demand'},
            {'id': 1001, 'name': 'Precio PVPC 2.0TD', 'type': 'price'},
            {'id': 1, 'name': 'Generación Hidráulica UGH', 'type': 'generation'},
            {'id': 4, 'name': 'Generación Nuclear', 'type': 'generation'},
            {'id': 9, 'name': 'Generación Ciclo Combinado', 'type': 'generation'},
            {'id': 12, 'name': 'Generación Eólica', 'type': 'generation'}
        ]
        
        all_data = []
        
        for indicator in key_indicators:
            print(f"\n📊 Recolectando: {indicator['name']} (ID: {indicator['id']})")
            
            # Obtener datos del indicador
            try:
                response = self.esios._make_request(f"indicators/{indicator['id']}")
                
                if response and 'indicator' in response:
                    indicator_data = response['indicator']
                    values = indicator_data.get('values', [])
                    
                    print(f"   Total valores disponibles: {len(values)}")
                    
                    # Procesar valores
                    for value in values:
                        all_data.append({
                            'indicator_id': indicator['id'],
                            'indicator_name': indicator['name'],
                            'indicator_type': indicator['type'],
                            'datetime': value.get('datetime', ''),
                            'value': value.get('value', ''),
                            'geo_id': value.get('geo_id', ''),
                            'geo_name': value.get('geo_name', ''),
                            'geo_agg': value.get('geo_agg', '')
                        })
                    
                    print(f"   ✅ Procesados {len(values)} valores")
                else:
                    print(f"   ❌ No se obtuvieron datos")
                
                # Respetar rate limiting
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error: {e}")
                continue
        
        if all_data:
            # Crear DataFrame
            df = pd.DataFrame(all_data)
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df.sort_values('datetime')
            
            print(f"\n📊 DATOS RECOLECTADOS:")
            print(f"   Total registros: {len(df)}")
            print(f"   Período: {df['datetime'].min()} a {df['datetime'].max()}")
            print(f"   Indicadores: {df['indicator_id'].nunique()}")
            
            # Guardar datos completos usando rutas centralizadas
            from config.paths import DATA_ESIOS_HISTORICAL
            filepath = DATA_ESIOS_HISTORICAL / 'extended_data.csv'
            df.to_csv(filepath, index=False, encoding='utf-8')
            print(f"   💾 Guardado en: {filepath}")
            
            # Crear dataset para ML
            self.create_ml_dataset_from_extended(df)
            
            return df
        else:
            print("❌ No se obtuvieron datos")
            return pd.DataFrame()
    
    def create_ml_dataset_from_extended(self, df: pd.DataFrame):
        """
        Crea dataset para ML a partir de los datos extendidos
        """
        print("\n🤖 CREANDO DATASET PARA ML")
        print("="*50)
        
        # Agrupar por fecha e indicador
        df['date'] = df['datetime'].dt.date
        df['hour'] = df['datetime'].dt.hour
        
        # Crear dataset consolidado
        ml_data = []
        
        # Obtener fechas únicas
        unique_dates = sorted(df['date'].unique())
        print(f"Procesando {len(unique_dates)} fechas...")
        
        for date in unique_dates:
            date_data = {'date': date}
            
            # Agregar características temporales
            date_obj = pd.to_datetime(date)
            date_data['year'] = date_obj.year
            date_data['month'] = date_obj.month
            date_data['day'] = date_obj.day
            date_data['dayofweek'] = date_obj.dayofweek
            date_data['is_weekend'] = 1 if date_obj.dayofweek in [5, 6] else 0
            date_data['quarter'] = date_obj.quarter
            date_data['day_of_year'] = date_obj.dayofyear
            date_data['week_of_year'] = date_obj.isocalendar().week
            
            # Procesar cada indicador para esta fecha
            date_df = df[df['date'] == date]
            
            for indicator_id in date_df['indicator_id'].unique():
                indicator_data = date_df[date_df['indicator_id'] == indicator_id]
                indicator_name = indicator_data['indicator_name'].iloc[0]
                indicator_type = indicator_data['indicator_type'].iloc[0]
                
                # Calcular estadísticas diarias
                if not indicator_data.empty:
                    date_data[f"{indicator_type}_{indicator_id}_mean"] = indicator_data['value'].mean()
                    date_data[f"{indicator_type}_{indicator_id}_max"] = indicator_data['value'].max()
                    date_data[f"{indicator_type}_{indicator_id}_min"] = indicator_data['value'].min()
                    date_data[f"{indicator_type}_{indicator_id}_std"] = indicator_data['value'].std()
                    date_data[f"{indicator_type}_{indicator_id}_count"] = len(indicator_data)
                    
                    # Agregar datos por hora
                    for hour in range(24):
                        hour_data = indicator_data[indicator_data['hour'] == hour]
                        if not hour_data.empty:
                            date_data[f"{indicator_type}_{indicator_id}_h{hour:02d}"] = hour_data['value'].mean()
            
            ml_data.append(date_data)
        
        # Crear DataFrame final
        ml_df = pd.DataFrame(ml_data)
        ml_df['date'] = pd.to_datetime(ml_df['date'])
        
        # Agregar características derivadas
        self.add_derived_features(ml_df)
        
        # Guardar dataset usando rutas centralizadas
        from config.paths import DATA_ESIOS_PROCESSED_HISTORICAL
        ml_filename = DATA_ESIOS_PROCESSED_HISTORICAL / 'extended_ml_dataset.csv'
        ml_df.to_csv(ml_filename, index=False, encoding='utf-8')
        
        print(f"✅ Dataset ML creado: {len(ml_df)} filas, {len(ml_df.columns)} columnas")
        print(f"💾 Guardado en: {ml_filename}")
        
        # Mostrar resumen
        print(f"\n📊 RESUMEN DEL DATASET:")
        print(f"   Período: {ml_df['date'].min().date()} a {ml_df['date'].max().date()}")
        print(f"   Días: {len(ml_df)}")
        print(f"   Variables: {len(ml_df.columns) - 1}")
        
        return ml_df
    
    def add_derived_features(self, df: pd.DataFrame):
        """
        Agrega características derivadas al dataset
        """
        print("🔧 Agregando características derivadas...")
        
        # Características de demanda
        demand_cols = [col for col in df.columns if col.startswith('demand_') and col.endswith('_mean')]
        if demand_cols:
            df['total_demand'] = df[demand_cols].sum(axis=1)
            df['demand_variability'] = df.get('demand_1293_std', 0) / df.get('demand_1293_mean', 1)
        
        # Características de precios
        price_cols = [col for col in df.columns if col.startswith('price_') and col.endswith('_mean')]
        if price_cols:
            df['avg_price'] = df[price_cols].mean(axis=1)
            df['price_variability'] = df.get('price_1001_std', 0) / df.get('price_1001_mean', 1)
        
        # Características de generación
        generation_cols = [col for col in df.columns if col.startswith('generation_') and col.endswith('_mean')]
        if generation_cols:
            df['total_generation'] = df[generation_cols].sum(axis=1)
            df['renewable_ratio'] = df.get('generation_12_mean', 0) / df['total_generation']
            df['nuclear_ratio'] = df.get('generation_4_mean', 0) / df['total_generation']
            df['thermal_ratio'] = df.get('generation_9_mean', 0) / df['total_generation']
        
        # Características temporales avanzadas
        df['is_month_start'] = df['date'].dt.is_month_start.astype(int)
        df['is_month_end'] = df['date'].dt.is_month_end.astype(int)
        df['is_quarter_start'] = df['date'].dt.is_quarter_start.astype(int)
        df['is_quarter_end'] = df['date'].dt.is_quarter_end.astype(int)
        
        print(f"✅ Características derivadas agregadas. Total columnas: {len(df.columns)}")

def create_simple_visualizations():
    """
    Crea visualizaciones simples de los datos
    """
    import matplotlib.pyplot as plt
    from config.paths import DATA_ESIOS_HISTORICAL, DATA_ESIOS_PROCESSED_HISTORICAL
    
    print("\n📊 CREANDO VISUALIZACIONES SIMPLES")
    print("="*50)
    
    try:
        # Cargar datos
        df = pd.read_csv(DATA_ESIOS_HISTORICAL / 'extended_data.csv')
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        # Configurar estilo
        plt.style.use('seaborn-v0_8')
        
        # 1. Evolución temporal de demanda
        demand_data = df[df['indicator_id'] == 1293]
        if not demand_data.empty:
            plt.figure(figsize=(15, 6))
            plt.plot(demand_data['datetime'], demand_data['value'], linewidth=1, alpha=0.7)
            plt.title('Evolución Temporal de la Demanda Real', fontsize=16, fontweight='bold')
            plt.xlabel('Fecha y Hora', fontsize=12)
            plt.ylabel('Demanda (MW)', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            from config.paths import DATA_ESIOS_PROCESSED_HISTORICAL
            plt.savefig(DATA_ESIOS_PROCESSED_HISTORICAL / 'demand_evolution.png', dpi=300, bbox_inches='tight')
            plt.show()
            print("✅ Demanda evolution guardada")
        
        # 2. Evolución temporal de precios
        price_data = df[df['indicator_id'] == 1001]
        if not price_data.empty:
            plt.figure(figsize=(15, 6))
            plt.plot(price_data['datetime'], price_data['value'], linewidth=1, alpha=0.7, color='red')
            plt.title('Evolución Temporal de Precios PVPC', fontsize=16, fontweight='bold')
            plt.xlabel('Fecha y Hora', fontsize=12)
            plt.ylabel('Precio (€/MWh)', fontsize=12)
            plt.grid(True, alpha=0.3)
            plt.xticks(rotation=45)
            plt.tight_layout()
            from config.paths import DATA_ESIOS_PROCESSED_HISTORICAL
            plt.savefig(DATA_ESIOS_PROCESSED_HISTORICAL / 'price_evolution.png', dpi=300, bbox_inches='tight')
            plt.show()
            print("✅ Precios evolution guardados")
        
        # 3. Comparación de generación por tipo
        generation_data = df[df['indicator_type'] == 'generation']
        if not generation_data.empty:
            generation_summary = generation_data.groupby(['indicator_id', 'indicator_name'])['value'].mean().reset_index()
            
            plt.figure(figsize=(12, 8))
            bars = plt.barh(range(len(generation_summary)), generation_summary['value'])
            plt.yticks(range(len(generation_summary)), 
                      [f"{row['indicator_name'][:30]}..." for _, row in generation_summary.iterrows()])
            plt.title('Generación Promedio por Tipo', fontsize=16, fontweight='bold')
            plt.xlabel('Generación Promedio (MW)', fontsize=12)
            plt.grid(True, alpha=0.3)
            
            # Agregar valores en las barras
            for i, bar in enumerate(bars):
                width = bar.get_width()
                plt.text(width + 50, bar.get_y() + bar.get_height()/2, 
                        f'{width:.0f} MW', ha='left', va='center')
            
            plt.tight_layout()
            from config.paths import DATA_ESIOS_PROCESSED_HISTORICAL
            plt.savefig(DATA_ESIOS_PROCESSED_HISTORICAL / 'generation_comparison.png', dpi=300, bbox_inches='tight')
            plt.show()
            print("✅ Comparación de generación guardada")
        
    except Exception as e:
        print(f"❌ Error creando visualizaciones: {e}")

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
    
    collector = ESIOSExtendedCollector(API_KEY)
    
    # Recolectar datos históricos extendidos
    df = collector.get_extended_historical_data(days_back=30)
    
    if not df.empty:
        # Crear visualizaciones
        create_simple_visualizations()
        
        print("\n✅ RECOLECCIÓN EXTENDIDA COMPLETADA")
        print("Archivos generados:")
        print("- data/raw/esios/historical/extended_data.csv")
        print("- data/processed/esios/historical/extended_ml_dataset.csv")
        print("- data/processed/esios/historical/*_evolution.png")
        print("- data/processed/esios/historical/generation_comparison.png")
    else:
        print("❌ No se pudieron recolectar datos")

if __name__ == "__main__":
    main()
