import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from esios_api import ESIOSAPI
import json
import requests
from datetime import datetime, timedelta

def test_different_endpoints():
    """
    Prueba diferentes endpoints de la API de e·sios
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return []
    base_url = "https://api.esios.ree.es"
    
    headers = {
        "x-api-key": API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    print("🧪 PROBANDO DIFERENTES ENDPOINTS DE LA API")
    print("="*60)
    
    # Endpoints a probar
    endpoints = [
        "/indicators",
        "/indicators/1001",
        "/indicators/1001/data",
        "/indicators/1002",
        "/indicators/1002/data",
        "/indicators/573",
        "/indicators/573/data",
        "/indicators/574",
        "/indicators/574/data",
        "/indicators/526",
        "/indicators/526/data",
        "/indicators/527",
        "/indicators/527/data",
        "/indicators/528",
        "/indicators/528/data",
        "/indicators/1004",
        "/indicators/1004/data",
        "/indicators/1005",
        "/indicators/1005/data",
    ]
    
    successful_endpoints = []
    
    for endpoint in endpoints:
        print(f"\n🔍 Probando: {endpoint}")
        
        try:
            url = f"{base_url}{endpoint}"
            response = requests.get(url, headers=headers)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Éxito")
                
                # Analizar la respuesta
                if isinstance(data, dict):
                    if 'indicators' in data:
                        print(f"   📊 Lista de indicadores: {len(data['indicators'])} elementos")
                    elif 'indicator' in data:
                        indicator = data['indicator']
                        print(f"   📊 Indicador: {indicator.get('name', 'N/A')}")
                        print(f"   📊 ID: {indicator.get('id', 'N/A')}")
                        print(f"   📊 Unidad: {indicator.get('unit', 'N/A')}")
                        if 'values' in indicator:
                            print(f"   📊 Valores: {len(indicator['values'])} elementos")
                elif isinstance(data, list):
                    print(f"   📊 Lista: {len(data)} elementos")
                
                successful_endpoints.append({
                    'endpoint': endpoint,
                    'status': response.status_code,
                    'data': data
                })
                
            elif response.status_code == 404:
                print(f"   ❌ No encontrado")
            elif response.status_code == 401:
                print(f"   ❌ No autorizado")
            elif response.status_code == 403:
                print(f"   ❌ Prohibido")
            else:
                print(f"   ⚠️  Status inesperado: {response.status_code}")
                print(f"   Respuesta: {response.text[:200]}...")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Guardar resultados exitosos
    if successful_endpoints:
        with open('data/raw/esios/successful_endpoints.json', 'w', encoding='utf-8') as f:
            json.dump(successful_endpoints, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n💾 Endpoints exitosos guardados en: data/raw/esios/successful_endpoints.json")
    
    return successful_endpoints

def test_data_endpoints_with_dates():
    """
    Prueba endpoints de datos con diferentes rangos de fechas
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return []
    base_url = "https://api.esios.ree.es"
    
    headers = {
        "x-api-key": API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }
    
    print("\n🗓️ PROBANDO ENDPOINTS DE DATOS CON FECHAS")
    print("="*60)
    
    # Fechas para probar
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Período: {start_str} a {end_str}")
    
    # Endpoints de datos a probar
    data_endpoints = [
        "/indicators/1001/data",
        "/indicators/1002/data",
        "/indicators/1003/data",
        "/indicators/1004/data",
        "/indicators/1005/data",
        "/indicators/526/data",
        "/indicators/527/data",
        "/indicators/528/data",
        "/indicators/573/data",
        "/indicators/574/data",
    ]
    
    successful_data = []
    
    for endpoint in data_endpoints:
        print(f"\n🔍 Probando: {endpoint}")
        
        try:
            url = f"{base_url}{endpoint}"
            params = {
                "start_date": start_str,
                "end_date": end_str
            }
            
            response = requests.get(url, headers=headers, params=params)
            
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Éxito")
                
                if 'indicator' in data and 'values' in data['indicator']:
                    values = data['indicator']['values']
                    print(f"   📊 Valores: {len(values)} elementos")
                    if values:
                        print(f"   📅 Primer valor: {values[0].get('datetime', 'N/A')}")
                        print(f"   📅 Último valor: {values[-1].get('datetime', 'N/A')}")
                
                successful_data.append({
                    'endpoint': endpoint,
                    'params': params,
                    'data': data
                })
                
            else:
                print(f"   ❌ Error: {response.status_code}")
                print(f"   Respuesta: {response.text[:200]}...")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Guardar datos exitosos
    if successful_data:
        with open('data/raw/esios/successful_data_endpoints.json', 'w', encoding='utf-8') as f:
            json.dump(successful_data, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n💾 Datos exitosos guardados en: data/raw/esios/successful_data_endpoints.json")
    
    return successful_data

def search_indicators_by_keywords():
    """
    Busca indicadores específicos por palabras clave
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return {}
    esios = ESIOSAPI(API_KEY)
    
    print("\n🔍 BUSCANDO INDICADORES POR PALABRAS CLAVE")
    print("="*60)
    
    # Obtener todos los indicadores
    indicators = esios.get_indicators()
    
    if not indicators or 'indicators' not in indicators:
        print("❌ No se pudieron obtener los indicadores")
        return
    
    all_indicators = indicators['indicators']
    
    # Palabras clave específicas para buscar
    keywords = [
        "demanda real",
        "demanda programada", 
        "precio medio",
        "pvpc",
        "tarifa",
        "consumo",
        "generación real",
        "generación programada"
    ]
    
    found_indicators = {}
    
    for keyword in keywords:
        print(f"\n🔍 Buscando: '{keyword}'")
        print("-" * 40)
        
        matches = []
        for indicator in all_indicators:
            name = indicator.get('name', '').lower()
            description = indicator.get('description', '').lower()
            
            if keyword.lower() in name or keyword.lower() in description:
                matches.append(indicator)
        
        print(f"Encontrados: {len(matches)} indicadores")
        
        # Mostrar los primeros 5
        for i, indicator in enumerate(matches[:5]):
            print(f"  {i+1:2d}. ID: {indicator.get('id', 'N/A'):4d} | {indicator.get('name', 'N/A')}")
        
        if len(matches) > 5:
            print(f"  ... y {len(matches) - 5} más")
        
        found_indicators[keyword] = matches
    
    # Guardar resultados
    with open('data/raw/esios/indicators_by_keywords.json', 'w', encoding='utf-8') as f:
        json.dump(found_indicators, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n💾 Resultados guardados en: data/raw/esios/indicators_by_keywords.json")
    
    return found_indicators

if __name__ == "__main__":
    # Probar diferentes endpoints
    successful_endpoints = test_different_endpoints()
    
    # Probar endpoints de datos con fechas
    successful_data = test_data_endpoints_with_dates()
    
    # Buscar indicadores por palabras clave
    found_indicators = search_indicators_by_keywords()
    
    print("\n✅ Pruebas completadas")
    print("Revisa los archivos generados en data/raw/esios/ para más detalles")

