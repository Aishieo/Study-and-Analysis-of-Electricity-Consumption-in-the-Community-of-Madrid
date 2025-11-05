# Módulos de Recopilación de Datos Adicionales

Este directorio contiene módulos para recopilar y procesar datos adicionales que enriquecen el análisis de consumo eléctrico por distritos en Madrid.

## 📁 Estructura de Archivos

### Colectores de Datos
- **`weather_data_collector.py`**: Recopila datos meteorológicos de Madrid
- **`electricity_prices_collector.py`**: Recopila datos de precios de electricidad
- **`air_quality_collector.py`**: Recopila datos de calidad del aire
- **`mobility_data_collector.py`**: Recopila datos de movilidad y transporte público

### Integración
- **`data_integration.py`**: Integra todos los datos adicionales con el dataset principal
- **`requirements_additional.txt`**: Dependencias adicionales necesarias

## 🚀 Uso Rápido

### 1. Instalar Dependencias
```bash
pip install -r src/requirements_additional.txt
```

### 2. Configurar Variables de Entorno (Opcional)
```bash
# Para datos meteorológicos de OpenWeatherMap
export OPENWEATHER_API_KEY="tu_api_key_aqui"
```

### 3. Ejecutar Recopilación de Datos
```python
from src.data_integration import DataIntegration

# Crear integrador
integrator = DataIntegration(openweather_api_key="tu_api_key")

# Recopilar todos los datos
additional_data = integrator.collect_all_additional_data(days_back=30)

# Integrar con datos principales
integrated_df = integrator.integrate_with_main_data(main_df, additional_data)
```

## 📊 Tipos de Datos Recopilados

### 🌤️ Datos Meteorológicos
- **Fuente**: OpenWeatherMap API + Ayuntamiento de Madrid
- **Variables**: Temperatura, humedad, presión, viento, precipitación, UV
- **Frecuencia**: Diaria
- **Cobertura**: 21 distritos de Madrid

### ⚡ Precios de Electricidad
- **Fuente**: OMIE (Operador del Mercado Ibérico de Energía)
- **Variables**: Precios por hora, tarifas punta/valle, costes estimados
- **Frecuencia**: Horaria
- **Cobertura**: Nacional (aplicado por distrito)

### 🌬️ Calidad del Aire
- **Fuente**: Ayuntamiento de Madrid
- **Variables**: NO₂, PM₁₀, PM₂.₅, O₃, SO₂, CO, ICA
- **Frecuencia**: Diaria
- **Cobertura**: 21 distritos de Madrid

### 🚌 Movilidad y Transporte
- **Fuente**: Consorcio Regional de Transportes + Ayuntamiento de Madrid
- **Variables**: Uso de metro/bus/cercanías, accesibilidad, densidad de tráfico
- **Frecuencia**: Diaria
- **Cobertura**: 21 distritos de Madrid

## 🔧 Funcionalidades Principales

### Recopilación Automática
- Descarga automática de datos de múltiples fuentes
- Manejo de rate limiting y errores
- Datos simulados como fallback

### Procesamiento Avanzado
- Cálculo de métricas estadísticas
- Normalización y estandarización
- Detección de outliers

### Integración Inteligente
- Fusión automática con datos principales
- Cálculo de índices compuestos
- Reportes de integración

## 📈 Métricas Calculadas

### Meteorológicas
- Temperatura media, mínima, máxima
- Humedad relativa promedio
- Presión atmosférica
- Velocidad del viento
- Precipitación acumulada

### Precios de Electricidad
- Precio medio por distrito
- Tarifas punta y valle
- Estimaciones de coste
- Diferenciales de precios

### Calidad del Aire
- Índice de Calidad del Aire (ICA)
- Concentraciones de contaminantes
- Días con calidad buena/mala
- Evaluación de impacto en salud

### Movilidad
- Uso de transporte público
- Puntuación de accesibilidad
- Conectividad urbana
- Densidad de tráfico

## 🎯 Índices Compuestos

### Índice de Sostenibilidad Ambiental
Combina calidad del aire y condiciones meteorológicas

### Índice de Eficiencia Energética
Relaciona consumo eléctrico con precios

### Índice de Accesibilidad Urbana
Combina accesibilidad y conectividad del transporte

### Índice de Calidad de Vida
Integra sostenibilidad, accesibilidad y renta

## 🔍 Ejemplos de Uso

### Análisis de Correlaciones
```python
# Correlación entre temperatura y consumo eléctrico
correlation = integrated_df['weather_temperatura_media_mean'].corr(integrated_df['p50'])
```

### Análisis de Clusters
```python
# Clustering basado en múltiples variables
from sklearn.cluster import KMeans
features = ['p50', 'weather_temperatura_media_mean', 'air_quality_ICA_mean']
kmeans = KMeans(n_clusters=3)
clusters = kmeans.fit_predict(integrated_df[features])
```

### Visualizaciones Avanzadas
```python
# Mapa de calor de correlaciones
import seaborn as sns
correlation_matrix = integrated_df.select_dtypes(include=[np.number]).corr()
sns.heatmap(correlation_matrix, annot=True)
```

## ⚠️ Consideraciones

### Limitaciones
- Algunos datos son simulados debido a limitaciones de API
- Las APIs pueden tener rate limits
- Los datos históricos pueden estar limitados

### Recomendaciones
- Configurar API keys para datos reales
- Ejecutar recopilación en horarios de baja demanda
- Validar datos antes de análisis críticos

## 📞 Soporte

Para problemas o preguntas sobre estos módulos:
1. Revisar los logs de error
2. Verificar configuración de APIs
3. Consultar documentación de las fuentes de datos

## 🔄 Actualizaciones

Los módulos están diseñados para:
- Actualización automática de datos
- Manejo de cambios en APIs
- Extensibilidad para nuevas fuentes de datos
