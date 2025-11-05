# 🔌 Estudio y Análisis del Consumo Eléctrico en la Comunidad de Madrid

Proyecto de análisis integral del consumo eléctrico por distritos en Madrid, combinando datos oficiales del INE, ESIOS (Red Eléctrica de España) y múltiples fuentes adicionales para comprender los factores que influyen en el consumo energético residencial.

## 📋 Tabla de Contenidos

- [Descripción](#-descripción)
- [Características](#-características)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Requisitos](#-requisitos)
- [Instalación](#-instalación)
- [Configuración](#-configuración)
- [Uso](#-uso)
- [Fuentes de Datos](#-fuentes-de-datos)
- [Estructura de Datos](#-estructura-de-datos)
- [Análisis Disponibles](#-análisis-disponibles)
- [Contribución](#-contribución)
- [Licencia](#-licencia)

## 🎯 Descripción

Este proyecto realiza un análisis exhaustivo del consumo eléctrico residencial en los 21 distritos de Madrid, integrando datos socioeconómicos, demográficos, meteorológicos, de calidad del aire, movilidad y precios de electricidad. El objetivo es identificar patrones, correlaciones y factores predictivos del consumo energético a nivel distrital.

### Objetivos

- Analizar el consumo eléctrico por percentiles (P10, P25, P50, P75, P90) en cada distrito
- Identificar factores socioeconómicos que influyen en el consumo
- Evaluar el impacto de variables ambientales (clima, calidad del aire)
- Estudiar correlaciones entre movilidad, renta y consumo energético
- Desarrollar modelos predictivos para el consumo eléctrico

## ✨ Características

### 🔄 Recopilación Automática de Datos

- **INE (Instituto Nacional de Estadística)**: Consumo eléctrico, renta, demografía, educación
- **ESIOS (Red Eléctrica de España)**: Demanda, precios, generación eléctrica
- **Datos Adicionales**: Meteorología, calidad del aire, movilidad, precios de electricidad
- **Integración Automática**: Combina múltiples fuentes en datasets unificados

### 📊 Análisis Incluidos

- Análisis exploratorio de datos (EDA) con visualizaciones
- Análisis de correlaciones entre variables
- Modelos de regresión para predicción
- Análisis de clustering por patrones de consumo
- Métricas e índices compuestos (sostenibilidad, eficiencia energética, calidad de vida)

### 🛠️ Herramientas

- Scripts Python modulares y reutilizables
- Notebooks Jupyter para análisis interactivos
- Sistema de logging centralizado
- Configuración centralizada de rutas y settings
- Manejo robusto de errores y rate limiting

## 📁 Estructura del Proyecto

```
TFM/
├── main.py                      # Script principal para ejecutar recopilación de datos
├── requirements.txt             # Dependencias Python
├── .gitignore                   # Archivos ignorados por Git
│
├── config/                      # Configuración compartida
│   ├── paths.py                 # Rutas centralizadas
│   └── __init__.py
│
├── src/                         # Código fuente
│   ├── config/                 # Configuración del proyecto
│   │   ├── paths.py            # Rutas de directorios
│   │   ├── settings.py         # Variables de entorno y configuración
│   │   └── logging_config.py   # Configuración de logging
│   │
│   ├── utils/                  # Utilidades
│   │   ├── madrid_districts.py # Mapeo de distritos de Madrid
│   │   ├── text_utils.py       # Utilidades de texto
│   │   └── file_utils.py       # Utilidades de archivos
│   │
│   ├── data_integration.py     # Integración de datos adicionales
│   ├── ine_api.py              # API del INE (renta, demografía)
│   ├── ine_api_electric.py     # API del INE (consumo eléctrico)
│   ├── esios_api.py            # Cliente API ESIOS
│   ├── esios_data_collector.py # Colector de datos ESIOS
│   ├── weather_data_collector.py
│   ├── air_quality_collector.py
│   ├── mobility_data_collector.py
│   └── electricity_prices_collector.py
│
├── notebooks/                   # Análisis en Jupyter
│   ├── EDA_Madrid.ipynb        # Análisis exploratorio
│   └── Regresiones_Distritos.ipynb
│
├── data/                        # Datos (no versionado)
│   ├── raw/                     # Datos sin procesar
│   │   ├── esios/              # Datos de ESIOS
│   │   └── *.xlsx              # Archivos Excel del INE
│   └── processed/              # Datos procesados
│       ├── *.csv               # Datos en CSV
│       └── esios/              # Análisis ESIOS
│
└── reports/                     # Reportes y visualizaciones
    └── figures/                 # Gráficos generados
```

## 📦 Requisitos

- **Python**: 3.8 o superior
- **Sistema Operativo**: Windows, Linux, macOS
- **Memoria**: Mínimo 4GB RAM recomendado
- **Espacio en Disco**: ~500MB para datos y dependencias

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/Aishieo/Study-and-Analysis-of-Electricity-Consumption-in-the-Community-of-Madrid.git
cd Study-and-Analysis-of-Electricity-Consumption-in-the-Community-of-Madrid
```

### 2. Crear entorno virtual (recomendado)

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/macOS
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Verificar instalación

```bash
python -c "import pandas, numpy, matplotlib, seaborn, sklearn; print('✅ Dependencias instaladas correctamente')"
```

## ⚙️ Configuración

### Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto (opcional, pero recomendado):

```bash
# API Keys (opcional)
ESIOS_API_KEY=tu_api_key_esios
OPENWEATHER_API_KEY=tu_api_key_openweather
```

O configura las variables de entorno en tu sistema:

```bash
# Windows (PowerShell)
$env:ESIOS_API_KEY="tu_api_key_esios"
$env:OPENWEATHER_API_KEY="tu_api_key_openweather"

# Linux/macOS
export ESIOS_API_KEY="tu_api_key_esios"
export OPENWEATHER_API_KEY="tu_api_key_openweather"
```

### Obtener API Keys

- **ESIOS**: [https://www.esios.ree.es/es/pagina/api](https://www.esios.ree.es/es/pagena/api)
- **OpenWeatherMap**: [https://openweathermap.org/api](https://openweathermap.org/api) (opcional, para datos meteorológicos)

**Nota**: Los datos del INE se descargan automáticamente, no requieren API key.

## 🎮 Uso

### Ejecutar Recopilación Completa de Datos

```bash
python main.py
```

Este comando ejecuta:
1. Recopilación de datos del INE (consumo, renta, demografía, educación)
2. Recopilación de datos de ESIOS (demanda, precios, generación)
3. Integración de datos adicionales (meteorología, calidad del aire, movilidad, precios)

### Ejecutar Colectores Específicos

```bash
# Solo datos del INE
python main.py --only ine

# Solo datos de ESIOS
python main.py --only esios

# Solo integración de datos adicionales
python main.py --only data_integration

# INE y ESIOS
python main.py --only ine esios
```

### Omitir Integración de Datos

```bash
python main.py --skip-integration
```

### Ver Logs

Los logs se guardan en `data_collection.log` y se muestran en consola.

### Trabajar con los Notebooks

```bash
# Iniciar Jupyter
jupyter notebook

# O abrir directamente
jupyter notebook notebooks/EDA_Madrid.ipynb
```

## 📊 Fuentes de Datos

### Datos Oficiales

| Fuente | Descripción | Datos |
|--------|------------|-------|
| **INE** | Instituto Nacional de Estadística | Consumo eléctrico (tabla 59532), Renta (31097), Demografía (31105), Educación (66753) |
| **ESIOS** | Red Eléctrica de España | Demanda, precios, generación eléctrica |
| **Ayuntamiento de Madrid** | Portal de datos abiertos | Calidad del aire, movilidad |

### Datos Adicionales

| Fuente | Descripción | Requiere API Key |
|--------|------------|------------------|
| **OpenWeatherMap** | Datos meteorológicos | ✅ Opcional |
| **OMIE** | Precios de electricidad | ❌ Público |

### Cobertura

- **Distritos**: 21 distritos de Madrid capital
- **Periodo**: Datos históricos disponibles según fuente
- **Frecuencia**: Diaria, horaria o según disponibilidad

## 📈 Estructura de Datos

### Datos Principales (INE)

- **Consumo Eléctrico**: Percentiles (P10, P25, P50, P75, P90) por distrito
- **Renta**: Renta neta/bruta media por persona y hogar
- **Demografía**: Edad media, población, hogares, nacionalidad
- **Educación**: Nivel educativo por sección censal

### Datos ESIOS

- **Demanda**: Demanda real, máxima, mínima
- **Precios**: Precios horarios, tarifas PVPC
- **Generación**: Por tipo de fuente (nuclear, eólica, solar, etc.)

### Datos Adicionales

- **Meteorología**: Temperatura, humedad, presión, viento, precipitación
- **Calidad del Aire**: NO₂, PM₁₀, PM₂.₅, O₃, ICA
- **Movilidad**: Estaciones de metro/bus, accesibilidad, conectividad

## 🔍 Análisis Disponibles

### Análisis Exploratorio (EDA)

- Distribuciones de consumo por distrito
- Correlaciones entre variables
- Análisis de outliers
- Visualizaciones geográficas

### Modelos Predictivos

- Regresión lineal múltiple
- Regresión con validación cruzada
- Análisis de residuos
- Métricas de evaluación

### Análisis de Clustering

- Agrupación por patrones de consumo
- Análisis de silueta
- Visualización de clusters

### Índices Compuestos

- **Índice de Sostenibilidad Ambiental**: Calidad del aire + condiciones meteorológicas
- **Índice de Eficiencia Energética**: Consumo / Precio
- **Índice de Accesibilidad Urbana**: Movilidad + conectividad
- **Índice de Calidad de Vida**: Integración de múltiples factores

## 🛠️ Desarrollo

### Estructura de Código

- **Modular**: Cada colector es independiente
- **Configurable**: Rutas y settings centralizados
- **Logging**: Sistema de logging unificado
- **Error Handling**: Manejo robusto de errores

### Agregar Nuevos Colectores

1. Crear archivo en `src/` siguiendo el patrón de los existentes
2. Implementar métodos de recopilación y procesamiento
3. Agregar al `data_integration.py` si es necesario
4. Actualizar `main.py` para incluir el nuevo colector

### Tests

```bash
# Ejecutar tests de endpoints ESIOS
python src/test_esios_endpoints.py

# Ejecutar tests de datos ESIOS
python src/test_esios_data.py
```

## 📝 Notas Importantes

### Rate Limiting

- Las APIs pueden tener límites de peticiones
- El código incluye delays automáticos
- Se recomienda ejecutar durante horarios de baja demanda

### Datos Simulados

- Algunos datos pueden ser simulados si las APIs no están disponibles
- Se marcan claramente en los datasets con columna `fuente`

### Archivos CSV

- Los archivos CSV generados no se versionan (ver `.gitignore`)
- Se generan automáticamente en `data/processed/`

## 🤝 Contribución

Las contribuciones son bienvenidas. Por favor:

1. Fork el proyecto
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

## 📄 Licencia

Este proyecto es parte de un Trabajo de Fin de Máster (TFM). Ver archivo LICENSE para más detalles.

## 👤 Autor

**Aishieo**

- GitHub: [@Aishieo](https://github.com/Aishieo)
- Email: alesantana2512@gmail.com

## 🙏 Agradecimientos

- **INE**: Por proporcionar datos oficiales de forma abierta
- **ESIOS/REE**: Por la API de datos energéticos
- **Ayuntamiento de Madrid**: Por el portal de datos abiertos
- **OpenWeatherMap**: Por la API meteorológica (opcional)

## 📚 Referencias

- [INE - Instituto Nacional de Estadística](https://www.ine.es/)
- [ESIOS - Red Eléctrica de España](https://www.esios.ree.es/)
- [Datos Abiertos Madrid](https://datos.madrid.es/)
- [OMIE - Operador del Mercado Ibérico de Energía](https://www.omie.es/)

---

⭐ Si este proyecto te resulta útil, considera darle una estrella en GitHub!
