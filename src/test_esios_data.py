import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from esios_api import ESIOSAPI
import json
import pandas as pd
from datetime import datetime, timedelta

def test_key_indicators():
    """
    Prueba indicadores clave que sabemos que tienen datos históricos
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return []
    esios = ESIOSAPI(API_KEY)
    
    print("🧪 PROBANDO INDICADORES CLAVE CON DATOS HISTÓRICOS")
    print("="*60)
    
    # Fechas para la prueba (últimos 30 días para tener más probabilidad de datos)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    start_str = start_date.strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')
    
    print(f"Período de prueba: {start_str} a {end_str}")
    
    # Indicadores clave que sabemos que tienen datos
    key_indicators = [
        # Demanda real
        {'id': 1001, 'name': 'Demanda programada PBF Total Nacional'},
        {'id': 1002, 'name': 'Demanda real Total Nacional'},
        {'id': 1003, 'name': 'Demanda real Total Nacional (sin desagregar)'},
        
        # Precios PVPC
        {'id': 526, 'name': 'Perfiles de consumo a efectos de facturación del PVPC Tarifa 2.0.A'},
        {'id': 527, 'name': 'Perfiles de consumo a efectos de facturación del PVPC Tarifa 2.0.DHA'},
        {'id': 528, 'name': 'Perfiles de consumo a efectos de facturación del PVPC Tarifa 2.0.DHS'},
        
        # Precios medios
        {'id': 573, 'name': 'Precio medio de la demanda en los SNP por sistema'},
        {'id': 574, 'name': 'Precio medio de la generación en los SNP por sistema'},
        
        # Generación total
        {'id': 1004, 'name': 'Generación programada PBF Total Nacional'},
        {'id': 1005, 'name': 'Generación real Total Nacional'},
    ]
    
    successful_indicators = []
    
    for indicator in key_indicators:
        print(f"\n🔍 Probando: {indicator['name']}")
        print(f"   ID: {indicator['id']}")
        
        try:
            data = esios.get_indicator_data(indicator['id'], start_str, end_str)
            
            if data and 'indicator' in data:
                values = data.get('indicator', {}).get('values', [])
                if values:
                    print(f"   ✅ Datos obtenidos: {len(values)} puntos")
                    
                    # Mostrar información del indicador
                    indicator_info = data.get('indicator', {})
                    print(f"   📊 Nombre: {indicator_info.get('name', 'N/A')}")
                    print(f"   📊 Descripción: {indicator_info.get('description', 'N/A')[:100]}...")
                    print(f"   📊 Unidad: {indicator_info.get('unit', 'N/A')}")
                    print(f"   📊 Tipo: {indicator_info.get('type', 'N/A')}")
                    
                    # Mostrar algunos valores de ejemplo
                    print(f"   📈 Primeros valores:")
                    for i, value in enumerate(values[:3]):
                        print(f"      {value.get('datetime', 'N/A')}: {value.get('value', 'N/A')} {indicator_info.get('unit', '')}")
                    
                    # Guardar datos exitosos
                    successful_indicators.append({
                        'id': indicator['id'],
                        'name': indicator['name'],
                        'data': data,
                        'values_count': len(values)
                    })
                else:
                    print(f"   ⚠️  Sin valores en el período especificado")
            else:
                print(f"   ❌ No se obtuvieron datos")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    # Guardar indicadores exitosos
    if successful_indicators:
        with open('data/raw/esios/successful_indicators.json', 'w', encoding='utf-8') as f:
            json.dump(successful_indicators, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n💾 Indicadores exitosos guardados en: data/raw/esios/successful_indicators.json")
        
        # Crear resumen
        print(f"\n📋 RESUMEN DE INDICADORES EXITOSOS:")
        print("-" * 50)
        for indicator in successful_indicators:
            print(f"✅ ID {indicator['id']}: {indicator['name']} ({indicator['values_count']} valores)")
    
    return successful_indicators

def test_specific_date_ranges():
    """
    Prueba diferentes rangos de fechas para encontrar datos
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return
    esios = ESIOSAPI(API_KEY)
    
    print("\n🗓️ PROBANDO DIFERENTES RANGOS DE FECHAS")
    print("="*50)
    
    # Probar diferentes rangos de fechas
    date_ranges = [
        ("Última semana", 7),
        ("Último mes", 30),
        ("Últimos 3 meses", 90),
        ("Último año", 365)
    ]
    
    # Indicador de prueba (demanda real)
    test_indicator_id = 1002
    test_indicator_name = "Demanda real Total Nacional"
    
    for range_name, days in date_ranges:
        print(f"\n🔍 Probando {range_name} ({days} días):")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        try:
            data = esios.get_indicator_data(test_indicator_id, start_str, end_str)
            
            if data and 'indicator' in data:
                values = data.get('indicator', {}).get('values', [])
                if values:
                    print(f"   ✅ {len(values)} valores encontrados")
                    print(f"   📅 Desde: {values[0].get('datetime', 'N/A')}")
                    print(f"   📅 Hasta: {values[-1].get('datetime', 'N/A')}")
                else:
                    print(f"   ⚠️  Sin valores")
            else:
                print(f"   ❌ Sin datos")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")

def explore_indicator_structure():
    """
    Explora la estructura de un indicador específico
    """
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        return
    esios = ESIOSAPI(API_KEY)
    
    print("\n🔍 EXPLORANDO ESTRUCTURA DE INDICADOR")
    print("="*50)
    
    # Obtener información detallada de un indicador
    indicator_id = 1002  # Demanda real Total Nacional
    print(f"Obteniendo información del indicador {indicator_id}...")
    
    try:
        info = esios.get_indicator_info(indicator_id)
        
        if info:
            print(f"\n📊 INFORMACIÓN DEL INDICADOR:")
            print(f"   ID: {info.get('indicator', {}).get('id', 'N/A')}")
            print(f"   Nombre: {info.get('indicator', {}).get('name', 'N/A')}")
            print(f"   Descripción: {info.get('indicator', {}).get('description', 'N/A')}")
            print(f"   Unidad: {info.get('indicator', {}).get('unit', 'N/A')}")
            print(f"   Tipo: {info.get('indicator', {}).get('type', 'N/A')}")
            print(f"   Frecuencia: {info.get('indicator', {}).get('frequency', 'N/A')}")
            print(f"   Fecha inicio: {info.get('indicator', {}).get('start_date', 'N/A')}")
            print(f"   Fecha fin: {info.get('indicator', {}).get('end_date', 'N/A')}")
            
            # Guardar información completa
            with open('data/raw/esios/indicator_1002_info.json', 'w', encoding='utf-8') as f:
                json.dump(info, f, ensure_ascii=False, indent=2, default=str)
            print(f"\n💾 Información guardada en: data/raw/esios/indicator_1002_info.json")
        else:
            print("❌ No se pudo obtener información del indicador")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    # Probar indicadores clave
    successful_indicators = test_key_indicators()
    
    # Probar diferentes rangos de fechas
    test_specific_date_ranges()
    
    # Explorar estructura de indicador
    explore_indicator_structure()
    
    print("\n✅ Pruebas completadas")
    print("Revisa los archivos generados en data/raw/esios/ para más detalles")

