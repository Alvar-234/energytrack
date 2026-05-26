# ⚡ EnergyTrack

**Sistema de Monitoreo y Análisis de Consumo Energético en Hogares Mexicanos**

> Proyecto Final — Ingeniería de Datos · Universidad Veracruzana 2026  
> Profesor: Benítez Guerrero Edgard Iván

---

## 👥 Equipo

| Nombre | Rol |
|--------|-----|
| Amador Suárez José Carlos | Desarrollo |
| Flores Nuñez Dylan | Desarrollo |
| Mendoza Prado Alvaro | Desarrollo |

---

## 📋 Descripción

EnergyTrack simula una red de medidores inteligentes en hogares mexicanos y procesa sus lecturas a través de una arquitectura completa de ingeniería de datos:

```
[Medidores simulados]
        │  lectura cada 3 segundos
        ▼
[Data Lake — Zona RAW]     ← Parquet particionado por hora
        │  Pipeline ETL cada 35 segundos
        ▼
[Data Lake — Zona Processed]  ← Parquet limpio por mes
        │  Carga incremental
        ▼
[Data Warehouse — PostgreSQL]  ← Esquema estrella
        │  Consultas analíticas
        ▼
[Dashboard interactivo]    ← Dash + Plotly + Reportes PDF
```

---

## 🏗️ Arquitectura

| Capa | Tecnología | Descripción |
|------|-----------|-------------|
| Streaming | Python `threading` | Dos hilos: productor (3s) y lake writer (30s) |
| Data Lake RAW | Apache Parquet | Datos crudos inmutables, particionados por hora |
| Data Lake Processed | Apache Parquet | Datos limpios post-ETL, particionados por mes |
| Pipeline ETL | Python + Pandas | Limpieza, transformación y carga incremental |
| Data Warehouse | PostgreSQL | Esquema estrella con 3 tablas de hechos y 5 vistas |
| Dashboard | Dash + Plotly | 4 pestañas + generación de reportes PDF |

---

## 📁 Estructura del proyecto

```
EnergyTrack/
├── config.py                  # Configuración central (rutas, tarifas CFE, medidores)
├── db.py                      # Módulo de conexión a PostgreSQL (SQLAlchemy + psycopg2)
├── simulador_streaming.py     # Genera lecturas en tiempo real → Data Lake RAW
├── pipeline.py                # ETL: Lake RAW → Lake Processed → Data Warehouse
├── dashboard.py               # Dashboard interactivo (lee solo del DWH)
├── requirements.txt           # Dependencias Python
└── data_lake/                 # Generado en tiempo de ejecución (en .gitignore)
    ├── raw/                   # Zona RAW: Parquet crudos del stream
    └── processed/             # Zona Processed: Parquet limpios post-ETL
```

---

## ⚙️ Requisitos previos

- **Python** 3.11 o superior
- **PostgreSQL** 14 o superior instalado y corriendo

---

## 🚀 Instalación

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

Crea la base de datos en PostgreSQL:

```sql
-- En psql o pgAdmin:
CREATE DATABASE energytrack;
```

Edita `config.py` con tus credenciales:

```python
PG_CONFIG = {
    "host":     "localhost",
    "port":     5432,          # cambia si usas otro puerto
    "dbname":   "energytrack",
    "user":     "postgres",
    "password": "TU_PASSWORD", # ← tu contraseña
}
```

---

## ▶️ Ejecución

El sistema requiere **3 terminales** abiertas simultáneamente:

### Terminal 1 — Pipeline ETL
```bash
python pipeline.py
```
> Inicializa el Data Warehouse en PostgreSQL (tablas + vistas) y comienza a procesar archivos del lake cada 35 segundos.

### Terminal 2 — Simulador de Streaming
```bash
python simulador_streaming.py
```
> Genera lecturas de los 8 medidores cada 3 segundos y las vuelca al Data Lake cada 30 segundos.

### Terminal 3 — Dashboard
```bash
python dashboard.py
```
> Abre el dashboard en: **http://127.0.0.1:8050**

---

## 📊 Dashboard

El dashboard tiene 4 pestañas, todas leyendo **exclusivamente del Data Warehouse**:

| Pestaña | Contenido |
|---------|-----------|
| 📊 Métricas | KPIs globales, consumo por región (tarifa CFE), ranking de hogares |
| 🔥 Consumo | Serie temporal diaria, curva de carga 24h, mapa de calor hora×día |
| ⚠️ Anomalías | Dispersión temporal de picos, picos por hora, top 10 anomalías |
| 📄 Reportes PDF | Genera y descarga reportes por hora / día / semana / mes |

---

## 🔌 Medidores simulados

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

Las tarifas corresponden a las tarifas domésticas reales de la CFE 2026, asignadas por temperatura media de verano de cada región.

---

## 🗄️ Esquema del Data Warehouse

```
dim_tiempo ──┐
dim_region ──┼──► fact_consumo          (una fila por lectura)
dim_hogar  ──┘
                  fact_resumen_diario   (agregados diarios pre-calculados)

Vistas analíticas:
  v_consumo_por_region    → kWh y costo por región y fecha
  v_consumo_por_hogar     → totales acumulados por hogar
  v_perfil_horario        → curva de carga promedio (0-23h)
  v_picos                 → detalle de anomalías detectadas
  v_resumen_general       → KPIs globales del sistema
```

---

## 🧹 Datos nulos y calidad

El simulador genera un **~5% de lecturas nulas** (configurado con `PROB_NULA = 0.05`) que representan fallas reales de medidores:

- `timeout_comunicacion` — Sin respuesta a tiempo
- `señal_debil` — Interferencia en transmisión
- `bateria_baja` — Batería insuficiente
- `reinicio_medidor` — Reinicio de firmware
- `corte_temporal` — Micro-corte eléctrico

Estas lecturas **se guardan en el Data Lake** para auditoría histórica, pero el ETL las filtra y **nunca llegan al Data Warehouse**.

---

## 🔄 Reiniciar desde cero

```bash
# Detener los 3 procesos (Ctrl+C)

# Windows
rmdir /s /q data_lake

# Linux / Mac
rm -rf data_lake/

# Volver a arrancar
python pipeline.py
python simulador_streaming.py
python dashboard.py
```

---

## 📦 Dependencias

```
pandas          — Manipulación de DataFrames en el ETL
numpy           — Generación de distribuciones de consumo
pyarrow         — Lectura/escritura de archivos Parquet
psycopg2-binary — Driver PostgreSQL para Python
sqlalchemy      — Engine para integración pandas + PostgreSQL
dash            — Framework del dashboard web
plotly          — Gráficas interactivas
dash-bootstrap-components — Componentes UI del dashboard
reportlab       — Generación de reportes PDF
```

---

## 📄 Licencia

MIT License — Universidad Veracruzana 2026
