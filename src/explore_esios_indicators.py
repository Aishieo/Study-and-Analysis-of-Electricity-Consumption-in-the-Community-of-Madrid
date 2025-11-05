import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from esios_api import ESIOSAPI
import json
import pandas as pd
from datetime import datetime, timedelta

def explore_specific_indicators():
    """
    Explora indicadores específicos relevantes para el análisis de Madrid
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return {}
    esios = ESIOSAPI(API_KEY)
    
    print("🔍 EXPLORANDO INDICADORES ESPECÍFICOS PARA MADRID")
    print("="*60)
    
    # Obtener todos los indicadores
    indicators = esios.get_indicators()
    
    if not indicators or 'indicators' not in indicators:
        print("❌ No se pudieron obtener los indicadores")
        return
    
    all_indicators = indicators['indicators']
    print(f"Total de indicadores disponibles: {len(all_indicators)}")
    
    # Palabras clave para buscar indicadores relevantes
    keywords = {
        'consumo': ['consumo', 'consumption', 'demanda', 'demand'],
        'precio': ['precio', 'price', 'pvpc', 'tarifa', 'coste', 'cost'],
        'generacion': ['generación', 'generation', 'producción', 'production'],
        'red': ['red', 'network', 'sistema', 'system', 'peninsular'],
        'madrid': ['madrid', 'comunidad', 'autónoma', 'ccaa']
    }
    
    relevant_indicators = {}
    
    for category, words in keywords.items():
        print(f"\n🔍 Buscando indicadores de {category.upper()}:")
        print("-" * 40)
        
        category_indicators = []
        for indicator in all_indicators:
            name = indicator.get('name', '').lower()
            description = indicator.get('description', '').lower()
            
            # Buscar coincidencias en nombre o descripción
            for word in words:
                if word in name or word in description:
                    category_indicators.append(indicator)
                    break
        
        # Mostrar los primeros 10 indicadores de cada categoría
        print(f"Encontrados {len(category_indicators)} indicadores:")
        for i, indicator in enumerate(category_indicators[:10]):
            print(f"  {i+1:2d}. ID: {indicator.get('id', 'N/A'):4d} | {indicator.get('name', 'N/A')}")
        
        if len(category_indicators) > 10:
            print(f"  ... y {len(category_indicators) - 10} más")
        
        relevant_indicators[category] = category_indicators
    
    # Buscar indicadores específicos de precios PVPC
    print(f"\n💰 INDICADORES ESPECÍFICOS DE PRECIOS PVPC:")
    print("-" * 50)
    
    pvpc_indicators = []
    for indicator in all_indicators:
        name = indicator.get('name', '').lower()
        if 'pvpc' in name and ('precio' in name or 'tarifa' in name):
            pvpc_indicators.append(indicator)
    
    for indicator in pvpc_indicators[:15]:  # Mostrar los primeros 15
        print(f"  ID: {indicator.get('id', 'N/A'):4d} | {indicator.get('name', 'N/A')}")
    
    # Buscar indicadores de demanda peninsular
    print(f"\n⚡ INDICADORES DE DEMANDA PENINSULAR:")
    print("-" * 50)
    
    demanda_indicators = []
    for indicator in all_indicators:
        name = indicator.get('name', '').lower()
        if 'demanda' in name and 'peninsular' in name:
            demanda_indicators.append(indicator)
    
    for indicator in demanda_indicators[:10]:
        print(f"  ID: {indicator.get('id', 'N/A'):4d} | {indicator.get('name', 'N/A')}")
    
    # Guardar resultados en un archivo JSON para referencia
    results = {
        'total_indicators': len(all_indicators),
        'categories': {},
        'pvpc_indicators': pvpc_indicators,
        'demanda_indicators': demanda_indicators
    }
    
    for category, indicators in relevant_indicators.items():
        results['categories'][category] = [
            {
                'id': ind.get('id'),
                'name': ind.get('name'),
                'description': ind.get('description', '')[:200] + '...' if len(ind.get('description', '')) > 200 else ind.get('description', '')
            }
            for ind in indicators
        ]
    
    # Guardar en archivo
    with open('data/raw/esios/indicators_exploration.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Resultados guardados en: data/raw/esios/indicators_exploration.json")
    
    return results

def get_sample_data():
    """
    Obtiene datos de muestra de algunos indicadores clave
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return {}
    esios = ESIOSAPI(API_KEY)
    
    print("\n📊 OBTENIENDO DATOS DE MUESTRA")
    print("="*50)
    
    # Fechas para la muestra (últimos 7 días)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Período: {start_str} a {end_str}")
    
    # Indicadores clave para probar
    key_indicators = [
        {'id': 573, 'name': 'Precio medio de la demanda en los SNP por sistema'},
        {'id': 574, 'name': 'Precio medio de la generación en los SNP por sistema'},
        {'id': 1001, 'name': 'Demanda programada PBF Total Nacional'},
        {'id': 1002, 'name': 'Demanda real Total Nacional'}
    ]
    
    sample_data = {}
    
    for indicator in key_indicators:
        print(f"\n🔍 Obteniendo datos para: {indicator['name']}")
        print(f"   ID: {indicator['id']}")
        
        try:
            data = esios.get_indicator_data(indicator['id'], start_str, end_str)
            
            if data and 'indicator' in data:
                print(f"   ✅ Datos obtenidos exitosamente")
                print(f"   📈 Puntos de datos: {len(data.get('indicator', {}).get('values', []))}")
                
                # Mostrar algunos valores de ejemplo
                values = data.get('indicator', {}).get('values', [])
                if values:
                    print(f"   📊 Primeros valores:")
                    for i, value in enumerate(values[:3]):
                        print(f"      {value.get('datetime', 'N/A')}: {value.get('value', 'N/A')}")
                
                sample_data[indicator['id']] = {
                    'name': indicator['name'],
                    'data': data
                }
            else:
                print(f"   ❌ No se obtuvieron datos")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Guardar datos de muestra
    if sample_data:
        with open('data/raw/esios/sample_data.json', 'w', encoding='utf-8') as f:
            json.dump(sample_data, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n💾 Datos de muestra guardados en: data/raw/esios/sample_data.json")
    
    return sample_data

if __name__ == "__main__":
    # Explorar indicadores
    results = explore_specific_indicators()
    
    # Obtener datos de muestra
    sample_data = get_sample_data()
    
    print("\n✅ Exploración completada")
    print("Revisa los archivos generados en data/raw/esios/ para más detalles")

