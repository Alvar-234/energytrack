"""
EnergyTrack — Configuración central
Todos los archivos importan desde aquí.
"""
from pathlib import Path

# Rutas
LAKE_RAW       = Path("data_lake/raw")
LAKE_PROCESSED = Path("data_lake/processed")
DWH_PATH       = Path("data_warehouse.db")
CHECKPOINT     = Path("data_lake/.checkpoint.json")


# Conexión a PostgreSQL (Data Warehouse)
PG_CONFIG = {
    "host":     "localhost",
    "port":     5433,
    "dbname":   "energytrack",
    "user":     "postgres",
    "password": "contraseña",   # cambiar por tu contraseña
}

# ==========================================
# Configuración de Apache Kafka
# ==========================================
KAFKA_CONFIG = {
    "bootstrap_servers": ["localhost:9092"],
    "topic_lecturas":    "energytrack-lecturas"
}
# ==========================================

# Ruta del stream buffer (sigue en SQLite — datos temporales) 
STREAM_DB = "energytrack_stream.db"

# Parámetros del simulador 
INTERVALO_LECTURA_S = 3    # segundos entre lecturas
INTERVALO_LAKE_S    = 30   # segundos entre volcados al lake

# Parámetros del pipeline 
INTERVALO_PIPELINE_S = 35

# Tarifas CFE reales 2026 
# Estructura por bloques (kWh/mes): básico, intermedio, excedente
# Fuente: CFE tarifas domésticas vigentes
TARIFAS_CFE = {
    "1": {
        "nombre":       "Tarifa 1",
        "descripcion":  "Clima templado (<25 °C verano)",
        "ciudades":     ["Ciudad de México"],
        "bloques": [
            {"limite_kwh": 75,   "precio": 0.954},   # básico
            {"limite_kwh": 140,  "precio": 1.013},   # intermedio bajo
            {"limite_kwh": 250,  "precio": 1.313},   # intermedio alto
            {"limite_kwh": None, "precio": 3.397},   # excedente
        ],
        "dac_kwh_mes":  500,   # umbral DAC mensual
        "dac_precio":   6.18,
    },
    "1B": {
        "nombre":       "Tarifa 1B",
        "descripcion":  "Calor moderado (≥28 °C verano)",
        "ciudades":     ["Guadalajara"],
        "bloques": [
            {"limite_kwh": 100,  "precio": 0.906},
            {"limite_kwh": 200,  "precio": 1.013},
            {"limite_kwh": 300,  "precio": 1.313},
            {"limite_kwh": None, "precio": 3.241},
        ],
        "dac_kwh_mes":  600,
        "dac_precio":   6.18,
    },
    "1E": {
        "nombre":       "Tarifa 1E",
        "descripcion":  "Extremadamente caluroso (≥32 °C verano)",
        "ciudades":     ["Monterrey"],
        "bloques": [
            {"limite_kwh": 300,  "precio": 0.785},
            {"limite_kwh": 500,  "precio": 0.944},
            {"limite_kwh": 700,  "precio": 1.313},
            {"limite_kwh": None, "precio": 2.942},
        ],
        "dac_kwh_mes":  850,
        "dac_precio":   6.18,
    },
    "1F": {
        "nombre":       "Tarifa 1F",
        "descripcion":  "De las más calientes (≥33 °C verano)",
        "ciudades":     ["Mérida"],
        "bloques": [
            {"limite_kwh": 300,  "precio": 0.741},
            {"limite_kwh": 500,  "precio": 0.900},
            {"limite_kwh": 700,  "precio": 1.313},
            {"limite_kwh": None, "precio": 2.849},
        ],
        "dac_kwh_mes":  1000,
        "dac_precio":   6.18,
    },
    "1C": {
        "nombre":       "Tarifa 1C",
        "descripcion":  "Calor fuerte (≥30 °C verano)",
        "ciudades":     ["Veracruz"],
        "bloques": [
            {"limite_kwh": 150,  "precio": 0.859},
            {"limite_kwh": 300,  "precio": 1.013},
            {"limite_kwh": 450,  "precio": 1.313},
            {"limite_kwh": None, "precio": 3.102},
        ],
        "dac_kwh_mes":  700,
        "dac_precio":   6.18,
    },
}

def tarifa_de_ciudad(ciudad: str) -> dict:
    """Devuelve la tarifa CFE correspondiente a una ciudad."""
    for clave, t in TARIFAS_CFE.items():
        if ciudad in t["ciudades"]:
            return t
    return TARIFAS_CFE["1"]  # default

def calcular_costo(kwh_acumulado_mes: float, kwh_intervalo: float,
                   tarifa: dict) -> float:
    """
    Calcula el costo de un intervalo dado el consumo acumulado del mes.
    Determina en qué bloque tarifario cae la lectura actual.
    Si el acumulado supera el umbral DAC, aplica tarifa DAC.
    """
    if kwh_acumulado_mes >= tarifa["dac_kwh_mes"]:
        return round(kwh_intervalo * tarifa["dac_precio"], 6)

    # Determinar bloque actual
    precio = tarifa["bloques"][-1]["precio"]  # default: excedente
    consumo_previo = 0
    for bloque in tarifa["bloques"]:
        if bloque["limite_kwh"] is None:
            precio = bloque["precio"]
            break
        if kwh_acumulado_mes < consumo_previo + bloque["limite_kwh"]:
            precio = bloque["precio"]
            break
        consumo_previo += bloque["limite_kwh"]

    return round(kwh_intervalo * precio, 6)

# Medidores activos 
# (id_medidor, id_hogar, ciudad, tarifa_clave, factor_consumo)
# factor_consumo: 1.0 = hogar promedio (250 kWh/mes)
#                 >1.0 = más consumo (casa grande, AC, etc.)
#                 <1.0 = menos consumo (depa pequeño)
MEDIDORES = [
    ("MED-001", "HOG-001", "Ciudad de México", "1",   0.8),  # depa pequeño
    ("MED-002", "HOG-002", "Ciudad de México", "1",   1.3),  # casa con AC
    ("MED-003", "HOG-003", "Guadalajara",      "1B",  1.1),
    ("MED-004", "HOG-004", "Guadalajara",      "1B",  0.7),  # depa
    ("MED-005", "HOG-005", "Monterrey",        "1E",  1.5),  # casa con AC
    ("MED-006", "HOG-006", "Monterrey",        "1E",  1.8),  # casa grande con AC
    ("MED-007", "HOG-007", "Mérida",           "1F",  1.4),  # calor extremo
    ("MED-008", "HOG-008", "Veracruz",         "1C",  1.0),
]