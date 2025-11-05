import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from esios_api import ESIOSAPI
import json
import pandas as pd
from datetime import datetime, timedelta
import time

class ESIOSDataCollector:
    """
    Colector de datos de la API de e·sios para Madrid
    """
    
    def __init__(self, api_key: str):
        self.esios = ESIOSAPI(api_key)
        self.api_key = api_key
        
    def get_indicator_data(self, indicator_id: int) -> dict:
        """
        Obtiene datos de un indicador específico
        """
        print(f"Obteniendo datos del indicador {indicator_id}...")
        
        try:
            # Obtener información del indicador (que incluye los valores)
            response = self.esios._make_request(f"indicators/{indicator_id}")
            
            if response and 'indicator' in response:
                indicator = response['indicator']
                values = indicator.get('values', [])
                
                print(f"✅ Datos obtenidos: {len(values)} valores")
                print(f"   Nombre: {indicator.get('name', 'N/A')}")
                print(f"   Unidad: {indicator.get('unit', 'N/A')}")
                print(f"   Tipo: {indicator.get('type', 'N/A')}")
                
                return {
                    'indicator_id': indicator_id,
                    'name': indicator.get('name', ''),
                    'unit': indicator.get('unit', ''),
                    'type': indicator.get('type', ''),
                    'description': indicator.get('description', ''),
                    'values': values
                }
            else:
                print(f"❌ No se obtuvieron datos para el indicador {indicator_id}")
                return None
                
        except Exception as e:
            print(f"❌ Error obteniendo datos del indicador {indicator_id}: {e}")
            return None
    
    def get_demand_data(self) -> dict:
        """
        Obtiene datos de demanda eléctrica
        """
        print("\n🔍 OBTENIENDO DATOS DE DEMANDA ELÉCTRICA")
        print("="*50)
        
        # Indicadores de demanda clave
        demand_indicators = [
            {'id': 1293, 'name': 'Demanda real'},
            {'id': 2037, 'name': 'Demanda real nacional'},
            {'id': 1740, 'name': 'Demanda Real SNP'},
            {'id': 624, 'name': 'Demanda real máximo diario'},
            {'id': 625, 'name': 'Demanda real mínimo diario'}
        ]
        
        demand_data = {}
        
        for indicator in demand_indicators:
            print(f"\n📊 {indicator['name']} (ID: {indicator['id']})")
            data = self.get_indicator_data(indicator['id'])
            if data:
                demand_data[indicator['id']] = data
            time.sleep(1)  # Respetar rate limiting
        
        return demand_data
    
    def get_price_data(self) -> dict:
        """
        Obtiene datos de precios eléctricos
        """
        print("\n💰 OBTENIENDO DATOS DE PRECIOS ELÉCTRICOS")
        print("="*50)
        
        # Indicadores de precios clave
        price_indicators = [
            {'id': 573, 'name': 'Precio medio de la demanda en los SNP por sistema'},
            {'id': 574, 'name': 'Precio medio de la generación en los SNP por sistema'},
            {'id': 1001, 'name': 'Término de facturación de energía activa del PVPC 2.0TD'},
            {'id': 1002, 'name': 'Periodo Tarifario del PVPC 2.0TD'},
            {'id': 1004, 'name': 'Término de facturación de energía activa del PVPC 2.0TD P2'},
            {'id': 1005, 'name': 'Término de facturación de energía activa del PVPC 2.0TD P3'}
        ]
        
        price_data = {}
        
        for indicator in price_indicators:
            print(f"\n💰 {indicator['name']} (ID: {indicator['id']})")
            data = self.get_indicator_data(indicator['id'])
            if data:
                price_data[indicator['id']] = data
            time.sleep(1)  # Respetar rate limiting
        
        return price_data
    
    def get_generation_data(self) -> dict:
        """
        Obtiene datos de generación eléctrica
        """
        print("\n⚡ OBTENIENDO DATOS DE GENERACIÓN ELÉCTRICA")
        print("="*50)
        
        # Indicadores de generación clave
        generation_indicators = [
            {'id': 1, 'name': 'Generación programada PBF Hidráulica UGH'},
            {'id': 4, 'name': 'Generación programada PBF Nuclear'},
            {'id': 9, 'name': 'Generación programada PBF Ciclo combinado'},
            {'id': 10, 'name': 'Generación programada PBF Fuel'},
            {'id': 11, 'name': 'Generación programada PBF Eólica'},
            {'id': 12, 'name': 'Generación programada PBF Solar fotovoltaica'}
        ]
        
        generation_data = {}
        
        for indicator in generation_indicators:
            print(f"\n⚡ {indicator['name']} (ID: {indicator['id']})")
            data = self.get_indicator_data(indicator['id'])
            if data:
                generation_data[indicator['id']] = data
            time.sleep(1)  # Respetar rate limiting
        
        return generation_data
    
    def save_data_to_csv(self, data: dict, filename: str):
        """
        Guarda los datos en formato CSV
        """
        print(f"\n💾 Guardando datos en {filename}...")
        
        all_data = []
        
        for indicator_id, indicator_data in data.items():
            values = indicator_data.get('values', [])
            
            for value in values:
                all_data.append({
                    'indicator_id': indicator_id,
                    'indicator_name': indicator_data.get('name', ''),
                    'unit': indicator_data.get('unit', ''),
                    'datetime': value.get('datetime', ''),
                    'value': value.get('value', ''),
                    'geo_id': value.get('geo_id', ''),
                    'geo_name': value.get('geo_name', ''),
                    'geo_agg': value.get('geo_agg', '')
                })
        
        if all_data:
            df = pd.DataFrame(all_data)
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"✅ Datos guardados: {len(all_data)} registros en {filename}")
        else:
            print(f"⚠️ No hay datos para guardar en {filename}")
    
    def collect_all_data(self):
        """
        Recolecta todos los datos disponibles
        """
        print("🚀 INICIANDO RECOLECCIÓN DE DATOS DE E·SIOS")
        print("="*60)
        
        # Usar rutas centralizadas (los directorios ya se crean automáticamente)
        from config.paths import DATA_ESIOS, DATA_ESIOS_PROCESSED
        
        # Recolectar datos de demanda
        demand_data = self.get_demand_data()
        if demand_data:
            from config.paths import DATA_ESIOS
            self.save_data_to_csv(demand_data, DATA_ESIOS / 'demand_data.csv')
            with open(DATA_ESIOS / 'demand_data.json', 'w', encoding='utf-8') as f:
                json.dump(demand_data, f, ensure_ascii=False, indent=2, default=str)
        
        # Recolectar datos de precios
        price_data = self.get_price_data()
        if price_data:
            from config.paths import DATA_ESIOS
            self.save_data_to_csv(price_data, DATA_ESIOS / 'price_data.csv')
            with open(DATA_ESIOS / 'price_data.json', 'w', encoding='utf-8') as f:
                json.dump(price_data, f, ensure_ascii=False, indent=2, default=str)
        
        # Recolectar datos de generación
        generation_data = self.get_generation_data()
        if generation_data:
            from config.paths import DATA_ESIOS
            self.save_data_to_csv(generation_data, DATA_ESIOS / 'generation_data.csv')
            with open(DATA_ESIOS / 'generation_data.json', 'w', encoding='utf-8') as f:
                json.dump(generation_data, f, ensure_ascii=False, indent=2, default=str)
        
        print("\n✅ RECOLECCIÓN COMPLETADA")
        print("Archivos generados:")
        print("- data/raw/esios/demand_data.csv")
        print("- data/raw/esios/price_data.csv") 
        print("- data/raw/esios/generation_data.csv")
        print("- Archivos JSON correspondientes")

def main():
    """
    Función principal para recolectar datos
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        print("   Por favor, configura ESIOS_API_KEY como variable de entorno")
        return
    
    collector = ESIOSDataCollector(API_KEY)
    collector.collect_all_data()

if __name__ == "__main__":
    main()

