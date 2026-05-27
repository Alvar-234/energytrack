# EnergyTrack

**Sistema de Monitoreo y Análisis de Consumo Energético en Hogares Mexicanos**

> Proyecto Final — Ingeniería de Datos · Universidad Veracruzana 2026  
> Profesor: Benítez Guerrero Edgard Iván

---

## Equipo

| Nombre |
|--------|
| Amador Suárez José Carlos |
| Flores Nuñez Dylan |
| Mendoza Prado Alvaro |

---

## Descripción

EnergyTrack simula una red de medidores inteligentes en hogares mexicanos y procesa sus lecturas a través de una **Arquitectura Kappa** con Apache Kafka como canal central de eventos. Todos los datos —tanto en tiempo real como históricos— fluyen por el mismo pipeline de streaming, eliminando la necesidad de una capa batch separada.

```
[Medidores simulados]
        │  evento JSON cada 3 segundos por medidor
        ▼
┌─────────────────────────────────────────┐
│     Apache Kafka (Docker)               │
│     Tópico: energytrack-lecturas        │  ← fuente de verdad
│     Kafdrop: http://localhost:9000      │  ← monitoreo web
└─────────────────────────────────────────┘
        │  micro-lote (8 eventos o cada 5s)
        ▼
[Pipeline ETL — KafkaConsumer]
  1. Limpieza (nulos, rangos físicos, estado != ok)
  2. Transformación (picos 3σ, costo CFE por bloques)
  3. Respaldo histórico → data_lake/processed/ (Parquet)
  4. Carga incremental → PostgreSQL (esquema estrella)
        │
        ▼
[Data Warehouse — PostgreSQL]
  dim_tiempo · dim_hogar · dim_region
  fact_consumo · fact_resumen_diario
  5 vistas analíticas
        │
        ▼
[Dashboard — Dash + Plotly]
  http://127.0.0.1:8050
  4 pestañas + reportes PDF descargables
```

---

## Arquitectura Kappa

La **Arquitectura Kappa** usa un único flujo de streaming como fuente de verdad, a diferencia de Lambda que mantiene capas batch y streaming separadas. En EnergyTrack:

- **Kafka** reemplaza el Data Lake RAW — cada evento publicado es inmutable y replayable
- El **consumer group** gestiona los offsets automáticamente (reemplaza el checkpoint JSON)
- Para reprocesar datos históricos basta con reiniciar el consumer con `auto_offset_reset='earliest'`
- El **Data Lake Processed** se conserva como respaldo de auditoría, no como fuente primaria

| Componente | Tecnología | Función |
|------------|-----------|---------|
| Broker de eventos | Apache Kafka 7.3 | Canal central de streaming |
| Coordinador | Apache Zookeeper 7.3 | Gestión del cluster Kafka |
| UI de monitoreo | Kafdrop | Visualización de tópicos y mensajes |
| Productor | Python `kafka-python` | Envía lecturas al tópico |
| Consumidor/ETL | Python `kafka-python` | Lee, transforma y carga al DWH |
| Data Warehouse | PostgreSQL | Esquema estrella analítico |
| Dashboard | Dash + Plotly | Visualización desde el DWH |

---

## Estructura del proyecto

```
EnergyTrack/
├── config.py                  # Configuración central (Kafka, PostgreSQL, tarifas CFE)
├── db.py                      # Módulo de conexión a PostgreSQL (SQLAlchemy + psycopg2)
├── simulador_streaming.py     # Productor Kafka: genera lecturas → tópico
├── pipeline.py                # Consumidor Kafka: ETL → Lake Processed → DWH
├── dashboard.py               # Dashboard interactivo (lee solo del DWH)
├── docker-compose.yml         # Infraestructura: Zookeeper + Kafka + Kafdrop
├── requirements.txt           # Dependencias Python
└── data_lake/
    └── processed/             # Respaldo histórico post-ETL (Parquet por mes)
```

---

## Requisitos previos

- **Python** 3.11 o superior
- **Docker Desktop** instalado y corriendo
- **PostgreSQL** 14 o superior

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/energytrack.git
cd energytrack
```

### 2. Instalar dependencias Python

```bash
pip install -r requirements.txt
```

### 3. Configurar PostgreSQL

Crea la base de datos:

```sql
-- En psql o pgAdmin:
CREATE DATABASE energytrack;
```

Edita `config.py` con tus credenciales:

```python
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "energytrack",
    "user":     "postgres",
    "password": "TU_PASSWORD",
}
```

### 4. Configurar Kafka (opcional)

El tópico y broker están en `config.py`. El valor por defecto funciona con el `docker-compose.yml` incluido:

```python
KAFKA_CONFIG = {
    "bootstrap_servers": ["localhost:9092"],
    "topic_lecturas":    "energytrack-lecturas",
}
```

---

## Ejecución

### Paso 1 — Levantar la infraestructura Kafka (Docker)

```bash
docker-compose up -d
```

Espera ~15 segundos a que Kafka esté listo. Puedes verificarlo en:
- **Kafdrop** (UI de Kafka): http://localhost:9000

Para detener Kafka al terminar:

```bash
docker-compose down
```

### Paso 2 — Arrancar los 3 componentes Python

Abre **3 terminales** en la carpeta del proyecto:

**Terminal 1 — Pipeline ETL (Consumidor Kafka)**
```bash
python pipeline.py
```
> Inicializa el DWH en PostgreSQL y comienza a escuchar eventos de Kafka.

**Terminal 2 — Simulador (Productor Kafka)**
```bash
python simulador_streaming.py
```
> Publica lecturas de 8 medidores cada 3 segundos en el tópico `energytrack-lecturas`.

**Terminal 3 — Dashboard**
```bash
python dashboard.py
```
> Abre el dashboard en: **http://127.0.0.1:8050**

---

## Dashboard

El dashboard tiene 4 pestañas, todas leyendo **exclusivamente del Data Warehouse (PostgreSQL)**:

| Pestaña | Vista DWH usada | Contenido |
|---------|----------------|-----------|
| Métricas | `v_resumen_general`, `v_consumo_por_region`, `v_consumo_por_hogar` | KPIs, barras por tarifa CFE, ranking de hogares |
| Consumo | `v_consumo_por_region`, `v_perfil_horario`, `fact_consumo` | Serie temporal, curva de carga, mapa de calor |
| Anomalías | `v_picos`, `v_perfil_horario` | Dispersión de picos, top 10 anomalías |
| Reportes PDF | Todas las vistas | Genera PDF descargable por hora / día / semana / mes |

---

## Medidores simulados

| Medidor | Ciudad | Tarifa CFE | Factor | kWh/mes est. |
|---------|--------|-----------|--------|-------------|
| MED-001 | Ciudad de México | 1 (Templado) | 0.8 | 200 kWh |
| MED-002 | Ciudad de México | 1 (Templado) | 1.3 | 325 kWh |
| MED-003 | Guadalajara | 1B (Calor mod.) | 1.1 | 275 kWh |
| MED-004 | Guadalajara | 1B (Calor mod.) | 0.7 | 175 kWh |
| MED-005 | Monterrey | 1E (Ext. cal.) | 1.5 | 375 kWh |
| MED-006 | Monterrey | 1E (Ext. cal.) | 1.8 | 450 kWh |
| MED-007 | Mérida | 1F (Máx. cal.) | 1.4 | 350 kWh |
| MED-008 | Veracruz | 1C (Cal. fuerte) | 1.0 | 250 kWh |

Las tarifas corresponden a las tarifas domésticas reales de la CFE 2026, con estructura de bloques subsidiados según temperatura media de verano de cada región.

---

## Esquema del Data Warehouse

```
dim_tiempo ──┐
dim_region ──┼──► fact_consumo          (una fila por lectura)
dim_hogar  ──┘         │
                       └──► fact_resumen_diario  (agregados diarios)

Vistas analíticas pre-construidas:
  v_consumo_por_region  → kWh y costo por tarifa CFE y fecha
  v_consumo_por_hogar   → totales acumulados por hogar
  v_perfil_horario      → curva de carga promedio (0–23h)
  v_picos               → detalle de anomalías detectadas (3σ)
  v_resumen_general     → KPIs globales del sistema
```

---

## Datos nulos y calidad

El simulador genera un **~5% de lecturas nulas** que representan fallas reales de medidores:

| Causa | Descripción |
|-------|-------------|
| `timeout_comunicacion` | Sin respuesta en tiempo esperado |
| `señal_debil` | Interferencia en la transmisión |
| `bateria_baja` | Batería insuficiente del medidor |
| `reinicio_medidor` | Reinicio de firmware |
| `corte_temporal` | Micro-corte eléctrico (<1s) |

Estas lecturas se **publican en Kafka** como parte del flujo real, pero el ETL las filtra (`estado != 'ok'` y `kwh_intervalo IS NULL`) y **nunca llegan al Data Warehouse**.

---

## Reiniciar desde cero

```bash
# 1. Detener los 3 procesos (Ctrl+C)

# 2. Reiniciar Kafka (limpia todos los mensajes del tópico)
docker-compose down -v
docker-compose up -d

# 3. Limpiar el Data Lake Processed (respaldo histórico)
# Windows:
rmdir /s /q data_lake

# Linux / Mac:
rm -rf data_lake/

# 4. Limpiar las tablas del DWH en PostgreSQL
# En psql o pgAdmin:
DROP TABLE IF EXISTS fact_resumen_diario, fact_consumo, dim_tiempo, dim_hogar, dim_region CASCADE;

# 5. Volver a arrancar
python pipeline.py
python simulador_streaming.py
python dashboard.py
```

### Reprocesar datos históricos de Kafka

Kafka conserva los mensajes por defecto 7 días. Para reprocesar desde el inicio sin borrar nada:

```bash
# Cambiar el group_id en pipeline.py a uno nuevo (ej. 'v3')
# Kafka asignará el offset a 'earliest' automáticamente
group_id='energytrack-pipeline-group-v3'
```

---

## Dependencias

| Paquete | Uso |
|---------|-----|
| `kafka-python` | Productor y consumidor Kafka (Arquitectura Kappa) |
| `pandas` | Manipulación de DataFrames en el ETL |
| `numpy` | Generación de distribuciones de consumo realistas |
| `pyarrow` | Lectura/escritura de archivos Parquet (Lake Processed) |
| `psycopg2-binary` | Driver PostgreSQL para escritura masiva |
| `sqlalchemy` | Engine para integración pandas + PostgreSQL |
| `dash` | Framework del dashboard web interactivo |
| `plotly` | Gráficas interactivas |
| `dash-bootstrap-components` | Componentes UI del dashboard |
| `reportlab` | Generación programática de reportes PDF |

---