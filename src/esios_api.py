import requests
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import pandas as pd

class ESIOSAPI:
    """
    Cliente para la API de e·sios de REE (Red Eléctrica de España)
    
    IMPORTANTE: Este token es para uso personal. No realizar peticiones masivas
    o redundantes. Respetar las restricciones de uso de la API.
    """
    
    def __init__(self, api_key: str, environment: str = "pro"):
        self.api_key = api_key
        self.environment = environment
        self.base_url = "https://api.esios.ree.es"
        self.headers = {
            "x-api-key": api_key,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.last_request_time = 0
        self.min_request_interval = 1  # Segundos entre peticiones para respetar la API
        
    def _rate_limit(self):
        """Controla el rate limiting para respetar las restricciones de la API"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            time.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
    
    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """
        Realiza una petición a la API con control de rate limiting
        """
        self._rate_limit()
        
        url = f"{self.base_url}/{endpoint}"
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error en la petición a {url}: {e}")
            return {}
    
    def get_indicators(self) -> Dict:
        """
        Obtiene la lista de indicadores disponibles en la API
        """
        print("Obteniendo lista de indicadores disponibles...")
        return self._make_request("indicators")
    
    def get_indicator_info(self, indicator_id: int) -> Dict:
        """
        Obtiene información detallada de un indicador específico
        """
        print(f"Obteniendo información del indicador {indicator_id}...")
        return self._make_request(f"indicators/{indicator_id}")
    
    def get_indicator_data(self, indicator_id: int, start_date: str, end_date: str) -> Dict:
        """
        Obtiene datos de un indicador para un rango de fechas
        
        Args:
            indicator_id: ID del indicador
            start_date: Fecha de inicio en formato YYYY-MM-DD
            end_date: Fecha de fin en formato YYYY-MM-DD
        """
        print(f"Obteniendo datos del indicador {indicator_id} desde {start_date} hasta {end_date}...")
        
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        
        return self._make_request(f"indicators/{indicator_id}/data", params)
    
    def get_indicators_by_type(self, indicator_type: str) -> List[Dict]:
        """
        Busca indicadores por tipo (ej: 'consumo', 'demanda', 'precio')
        """
        print(f"Buscando indicadores de tipo: {indicator_type}")
        indicators = self.get_indicators()
        
        if not indicators or 'indicators' not in indicators:
            return []
        
        # Filtrar por tipo en el nombre o descripción
        filtered = []
        for indicator in indicators['indicators']:
            name = indicator.get('name', '').lower()
            description = indicator.get('description', '').lower()
            if indicator_type.lower() in name or indicator_type.lower() in description:
                filtered.append(indicator)
        
        return filtered
    
    def test_connection(self) -> bool:
        """
        Prueba la conexión con la API
        """
        print("Probando conexión con la API de e·sios...")
        try:
            response = self._make_request("indicators")
            if response and 'indicators' in response:
                print("✅ Conexión exitosa con la API de e·sios")
                print(f"Total de indicadores disponibles: {len(response['indicators'])}")
                return True
            else:
                print("❌ Error en la conexión con la API")
                return False
        except Exception as e:
            print(f"❌ Error al conectar con la API: {e}")
            return False

def main():
    """
    Función principal para probar la conexión y explorar los datos disponibles
    """
    # Configuración de la API (desde variable de entorno)
    from config.settings import get_api_key
    try:
        API_KEY = get_api_key("ESIOS")
    except ValueError as e:
        print(f"❌ Error de configuración: {e}")
        print("   Por favor, configura ESIOS_API_KEY como variable de entorno")
        return
    
    # Crear instancia del cliente
    esios = ESIOSAPI(API_KEY)
    
    # Probar conexión
    if not esios.test_connection():
        return
    
    print("\n" + "="*50)
    print("EXPLORANDO INDICADORES DISPONIBLES")
    print("="*50)
    
    # Obtener todos los indicadores
    indicators = esios.get_indicators()
    
    if indicators and 'indicators' in indicators:
        print(f"\nTotal de indicadores: {len(indicators['indicators'])}")
        
        # Mostrar algunos indicadores de ejemplo
        print("\nPrimeros 10 indicadores:")
        for i, indicator in enumerate(indicators['indicators'][:10]):
            print(f"{i+1}. ID: {indicator.get('id', 'N/A')}")
            print(f"   Nombre: {indicator.get('name', 'N/A')}")
            print(f"   Descripción: {indicator.get('description', 'N/A')[:100]}...")
            print()
        
        # Buscar indicadores específicos
        print("\n" + "="*50)
        print("BUSCANDO INDICADORES ESPECÍFICOS")
        print("="*50)
        
        # Buscar indicadores de consumo
        consumo_indicators = esios.get_indicators_by_type("consumo")
        print(f"\nIndicadores de consumo encontrados: {len(consumo_indicators)}")
        for indicator in consumo_indicators[:5]:  # Mostrar solo los primeros 5
            print(f"- {indicator.get('name', 'N/A')} (ID: {indicator.get('id', 'N/A')})")
        
        # Buscar indicadores de demanda
        demanda_indicators = esios.get_indicators_by_type("demanda")
        print(f"\nIndicadores de demanda encontrados: {len(demanda_indicators)}")
        for indicator in demanda_indicators[:5]:
            print(f"- {indicator.get('name', 'N/A')} (ID: {indicator.get('id', 'N/A')})")
        
        # Buscar indicadores de precio
        precio_indicators = esios.get_indicators_by_type("precio")
        print(f"\nIndicadores de precio encontrados: {len(precio_indicators)}")
        for indicator in precio_indicators[:5]:
            print(f"- {indicator.get('name', 'N/A')} (ID: {indicator.get('id', 'N/A')})")

if __name__ == "__main__":
    main()