"""
EnergyTrack — Pipeline ETL (Versión Streaming / Arquitectura Kappa)
Lee directamente de Apache Kafka → ETL → Data Warehouse (PostgreSQL)

EJECUTAR: python pipeline.py
"""

import json
import time
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras
from config import (LAKE_PROCESSED, MEDIDORES, TARIFAS_CFE, tarifa_de_ciudad,
                    calcular_costo, INTERVALO_PIPELINE_S, PG_CONFIG, KAFKA_CONFIG)
from db import get_dwh_conn, dwh_query, dwh_execute, dwh_execute_script

# ══════════════════════════════════════════════════════════
#  DDL — Esquema del Data Warehouse (INTACTO)
# ══════════════════════════════════════════════════════════

DDL = [
    """
    CREATE TABLE IF NOT EXISTS dim_tiempo (
        tiempo_key  BIGINT PRIMARY KEY,
        timestamp   TEXT,
        fecha       DATE,
        year        INTEGER,
        month       INTEGER,
        day         INTEGER,
        hora        INTEGER,
        dia_semana  TEXT,
        es_finde    BOOLEAN,
        trimestre   INTEGER,
        mes_nombre  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_region (
        region_key  SERIAL PRIMARY KEY,
        region      TEXT UNIQUE,
        tarifa_kwh  DOUBLE PRECISION
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dim_hogar (
        hogar_key   SERIAL PRIMARY KEY,
        id_hogar    TEXT UNIQUE,
        id_medidor  TEXT,
        ciudad      TEXT,
        region_key  INTEGER REFERENCES dim_region(region_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_consumo (
        id              BIGSERIAL PRIMARY KEY,
        tiempo_key      BIGINT REFERENCES dim_tiempo(tiempo_key),
        hogar_key       INTEGER REFERENCES dim_hogar(hogar_key),
        region_key      INTEGER REFERENCES dim_region(region_key),
        timestamp_real  TIMESTAMP,
        kwh_intervalo   DOUBLE PRECISION,
        voltaje         DOUBLE PRECISION,
        corriente       DOUBLE PRECISION,
        factor_potencia DOUBLE PRECISION,
        potencia_w      DOUBLE PRECISION,
        costo_mxn       DOUBLE PRECISION,
        es_pico         BOOLEAN DEFAULT FALSE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS fact_resumen_diario (
        id           BIGSERIAL PRIMARY KEY,
        fecha        DATE,
        hogar_key    INTEGER REFERENCES dim_hogar(hogar_key),
        region_key   INTEGER REFERENCES dim_region(region_key),
        kwh_total    DOUBLE PRECISION,
        kwh_max      DOUBLE PRECISION,
        kwh_promedio DOUBLE PRECISION,
        costo_total  DOUBLE PRECISION,
        num_lecturas INTEGER,
        num_picos    INTEGER
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_fc_tiempo  ON fact_consumo(tiempo_key)",
    "CREATE INDEX IF NOT EXISTS idx_fc_hogar   ON fact_consumo(hogar_key)",
    "CREATE INDEX IF NOT EXISTS idx_fc_region  ON fact_consumo(region_key)",
    "CREATE INDEX IF NOT EXISTS idx_fc_pico    ON fact_consumo(es_pico)",
    "CREATE INDEX IF NOT EXISTS idx_fc_ts      ON fact_consumo(timestamp_real)",
    "CREATE INDEX IF NOT EXISTS idx_frd_fecha  ON fact_resumen_diario(fecha)",
]

VISTAS = {
"v_consumo_por_region": """
    SELECT r.region, t.fecha::text AS fecha,
           SUM(f.kwh_intervalo)  AS kwh_total,
           AVG(f.kwh_intervalo)  AS kwh_promedio,
           SUM(f.costo_mxn)      AS costo_total,
           SUM(CASE WHEN f.es_pico THEN 1 ELSE 0 END) AS num_picos,
           COUNT(*)              AS num_lecturas
    FROM fact_consumo f
    JOIN dim_region r ON f.region_key = r.region_key
    JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
    GROUP BY r.region, t.fecha
""",
"v_consumo_por_hogar": """
    SELECT h.id_hogar, h.ciudad, r.region,
           SUM(f.kwh_intervalo) AS kwh_total,
           SUM(f.costo_mxn)     AS costo_total,
           SUM(CASE WHEN f.es_pico THEN 1 ELSE 0 END) AS num_picos
    FROM fact_consumo f
    JOIN dim_hogar  h ON f.hogar_key  = h.hogar_key
    JOIN dim_region r ON f.region_key = r.region_key
    GROUP BY h.id_hogar, h.ciudad, r.region
""",
"v_perfil_horario": """
    SELECT t.hora,
           AVG(f.kwh_intervalo) AS kwh_promedio,
           MAX(f.kwh_intervalo) AS kwh_max,
           SUM(CASE WHEN f.es_pico THEN 1 ELSE 0 END) AS num_picos
    FROM fact_consumo f
    JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
    GROUP BY t.hora
    ORDER BY t.hora
""",
"v_picos": """
    SELECT f.timestamp_real::text AS timestamp_real,
           t.hora, h.id_hogar, h.ciudad, r.region,
           f.kwh_intervalo, f.costo_mxn
    FROM fact_consumo f
    JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
    JOIN dim_hogar  h ON f.hogar_key  = h.hogar_key
    JOIN dim_region r ON f.region_key = r.region_key
    WHERE f.es_pico = TRUE
    ORDER BY f.kwh_intervalo DESC
""",
"v_resumen_general": """
    SELECT
        COUNT(*)                         AS total_lecturas,
        SUM(kwh_intervalo)               AS kwh_total,
        AVG(kwh_intervalo)               AS kwh_promedio,
        SUM(costo_mxn)                   AS costo_total,
        SUM(CASE WHEN es_pico THEN 1 ELSE 0 END) AS total_picos,
        COUNT(DISTINCT hogar_key)        AS hogares_activos
    FROM fact_consumo
""",
}

def inicializar_dwh():
    dwh_execute_script(DDL)

    for nombre, sql in VISTAS.items():
        dwh_execute_script([f"CREATE OR REPLACE VIEW {nombre} AS {sql}"])

    for clave, t in TARIFAS_CFE.items():
        dwh_execute(
            "INSERT INTO dim_region (region, tarifa_kwh) VALUES (%s, %s) ON CONFLICT (region) DO NOTHING",
            (clave, t["bloques"][0]["precio"])
        )

    conn = get_dwh_conn()
    cur  = conn.cursor()
    cur.execute("SELECT region, region_key FROM dim_region")
    map_region = {r[0]: r[1] for r in cur.fetchall()}
    conn.close()

    for med_id, hog_id, ciudad, tarifa_clave, _ in MEDIDORES:
        dwh_execute(
            """INSERT INTO dim_hogar (id_hogar, id_medidor, ciudad, region_key)
               VALUES (%s, %s, %s, %s) ON CONFLICT (id_hogar) DO NOTHING""",
            (hog_id, med_id, ciudad, map_region.get(tarifa_clave))
        )

# ══════════════════════════════════════════════════════════
#  PASO 2 — ETL MODIFICADO (Recibe un DataFrame directo de Kafka)
# ══════════════════════════════════════════════════════════

def etl(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    if df.empty:
        return pd.DataFrame(), {}

    stats = {"filas_raw": len(df)}

    # Limpieza de nulos y anomalías eléctricas
    df = df.dropna(subset=["kwh_intervalo", "timestamp"])
    df = df[df["kwh_intervalo"].between(0, 5)]
    if "voltaje" in df.columns:
        df = df[df["voltaje"].between(100, 140)]
    if "estado" in df.columns:
        df = df[df["estado"] == "ok"]

    # Transformaciones temporales
    df["timestamp"]  = pd.to_datetime(df["timestamp"])
    df["hora"]       = df["timestamp"].dt.hour
    df["dia_semana"] = df["timestamp"].dt.day_name()
    df["es_finde"]   = df["timestamp"].dt.dayofweek >= 5

    # Detección de picos distributiva (3σ)
    media = df["kwh_intervalo"].mean()
    sigma = df["kwh_intervalo"].std()
    if pd.isna(sigma) or sigma == 0:
        sigma = 1.0  # Evita errores de división por cero si el lote es idéntico
        
    df["es_pico"] = (
        (df["kwh_intervalo"] > media + 3 * sigma) |
        (df.get("es_pico", pd.Series(0, index=df.index)).astype(bool))
    )

    # Costo CFE por bloques
    def costo_lectura(row):
        tarifa_clave = row.get("tarifa", "1") if "tarifa" in row.index else "1"
        tarifa = TARIFAS_CFE.get(tarifa_clave, TARIFAS_CFE["1"])
        return calcular_costo(0, float(row["kwh_intervalo"]), tarifa)

    df["costo_mxn"] = df.apply(costo_lectura, axis=1)

    stats["filas_limpias"]    = len(df)
    stats["filas_eliminadas"] = stats["filas_raw"] - len(df)
    stats["picos_detectados"] = int(df["es_pico"].sum())

    return df, stats

# ══════════════════════════════════════════════════════════
#  PASO 3 — RESPALDO HISTÓRICO (Mantiene tus datasets limpios)
# ══════════════════════════════════════════════════════════

def guardar_processed(df: pd.DataFrame) -> list[Path]:
    guardados = []
    df = df.copy()
    df["year"]  = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    for (y, m), grupo in df.groupby(["year", "month"]):
        ruta = LAKE_PROCESSED / f"year={y}" / f"month={m:02d}"
        ruta.mkdir(parents=True, exist_ok=True)
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        archivo = ruta / f"clean_{ts}.parquet"
        grupo.drop(columns=["year", "month"]).to_parquet(archivo, index=False)
        guardados.append(archivo)

    return guardados

# ══════════════════════════════════════════════════════════
#  PASO 4 — CARGA INCREMENTAL AL DWH (INTACTO)
# ══════════════════════════════════════════════════════════

MESES_ES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
            7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",
            11:"Noviembre",12:"Diciembre"}

def cargar_dwh(df: pd.DataFrame) -> int:
    conn = get_dwh_conn()
    cur  = conn.cursor()

    cur.execute("SELECT region, region_key FROM dim_region")
    map_region = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT id_hogar, hogar_key FROM dim_hogar")
    map_hogar = {r[0]: r[1] for r in cur.fetchall()}

    ts_unicos = df["timestamp"].dt.floor("15min").unique()
    dim_tiempo_rows = []
    for ts in ts_unicos:
        ts = pd.Timestamp(ts)
        key = int(ts.strftime("%Y%m%d%H%M"))
        dim_tiempo_rows.append((
            key, ts.isoformat(), ts.date(),
            ts.year, ts.month, ts.day, ts.hour,
            ts.day_name(), ts.dayofweek >= 5,
            (ts.month - 1) // 3 + 1,
            MESES_ES[ts.month],
        ))

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO dim_tiempo
            (tiempo_key, timestamp, fecha, year, month, day, hora,
             dia_semana, es_finde, trimestre, mes_nombre)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (tiempo_key) DO NOTHING
    """, dim_tiempo_rows, page_size=500)

    cur.execute("SELECT timestamp, tiempo_key FROM dim_tiempo")
    map_tiempo = {r[0]: r[1] for r in cur.fetchall()}

    med_hogar        = {med: hog for med, hog, *_ in MEDIDORES}
    med_tarifa_clave = {med: tar for med, _, _, tar, _ in MEDIDORES}
    hogar_tarifa     = {hog: tarifa_de_ciudad(ciudad) for _, hog, ciudad, *_ in MEDIDORES}

    df["ts_key"] = df["timestamp"].dt.floor("15min").dt.strftime("%Y-%m-%dT%H:%M:%S")
    hechos = []

    for _, row in df.iterrows():
        hog_id       = row.get("id_hogar") or med_hogar.get(row.get("id_medidor", ""), "")
        tarifa_clave = row.get("tarifa") or med_tarifa_clave.get(row.get("id_medidor", ""), "1")
        tarifa       = hogar_tarifa.get(hog_id) or TARIFAS_CFE.get(tarifa_clave, TARIFAS_CFE["1"])
        kwh          = float(row["kwh_intervalo"])

        hechos.append((
            map_tiempo.get(row["ts_key"]),
            map_hogar.get(hog_id),
            map_region.get(tarifa_clave),
            row["timestamp"],
            kwh,
            row.get("voltaje"),
            row.get("corriente"),
            row.get("factor_potencia"),
            row.get("potencia_w"),
            calcular_costo(0, kwh, tarifa),
            bool(row.get("es_pico", False)),
        ))

    psycopg2.extras.execute_batch(cur, """
        INSERT INTO fact_consumo
            (tiempo_key, hogar_key, region_key, timestamp_real,
             kwh_intervalo, voltaje, corriente, factor_potencia,
             potencia_w, costo_mxn, es_pico)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, hechos, page_size=500)

    fechas = df["timestamp"].dt.date.unique()
    for fecha in fechas:
        cur.execute("DELETE FROM fact_resumen_diario WHERE fecha = %s", (fecha,))

    cur.execute("""
        INSERT INTO fact_resumen_diario
            (fecha, hogar_key, region_key, kwh_total, kwh_max,
             kwh_promedio, costo_total, num_lecturas, num_picos)
        SELECT t.fecha, f.hogar_key, f.region_key,
               SUM(f.kwh_intervalo),  MAX(f.kwh_intervalo),
               AVG(f.kwh_intervalo),  SUM(f.costo_mxn),
               COUNT(*), SUM(CASE WHEN f.es_pico THEN 1 ELSE 0 END)
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        WHERE t.fecha = ANY(%s::date[])
        GROUP BY t.fecha, f.hogar_key, f.region_key
    """, ([str(f) for f in fechas],))

    conn.commit()
    cur.close()
    conn.close()
    return len(hechos)

# ══════════════════════════════════════════════════════════
#  EJECUCIÓN CONTINUA DEL STREAM
# ══════════════════════════════════════════════════════════

def ejecutar_ciclo(mensajes_raw: list[dict], num_ciclo: int):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n{'═'*55}")
    print(f"  [{ts}] Pipeline Streaming — Ventana #{num_ciclo}")
    print(f"{'═'*55}")
    print(f"  Eventos extraídos de Kafka: {len(mensajes_raw)}")

    df_raw = pd.DataFrame(mensajes_raw)
    df, stats = etl(df_raw)

    if df.empty:
        print("  ✗ El flujo actual solo contenía datos inválidos o nulos.")
        return

    print(f"    · Procesados : {stats['filas_raw']:,}")
    print(f"    · Limpios    : {stats['filas_limpias']:,}  ({stats['filas_eliminadas']} eliminados)")
    print(f"    · Picos      : {stats['picos_detectados']}")

    # Mantiene la generación de datasets limpios para la entrega de tu proyecto
    archivos_proc = guardar_processed(df)
    for a in archivos_proc:
        print(f"    → Guardado respaldo en Lake Processed: {a.name}")

    # Inserción incremental en PostgreSQL
    n_insertados = cargar_dwh(df)
    print(f"    · Registros insertados en fact_consumo: {n_insertados:,}")

    # Monitor de Totales en el DWH
    resumen = dwh_query("SELECT COUNT(*) AS total, ROUND(SUM(kwh_intervalo)::numeric, 4) AS kwh FROM fact_consumo")
    if not resumen.empty:
        row = resumen.iloc[0]
        print(f"    · Total acumulado en DWH: {int(row['total']):,} filas | {row['kwh']} kWh")

# ══════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    LAKE_PROCESSED.mkdir(parents=True, exist_ok=True)

    print("=" * 55)
    print("  EnergyTrack — Pipeline ETL (CONSUMIDOR STREAMING)")
    print("=" * 55)
    print(f"  Broker Kafka   : {KAFKA_CONFIG['bootstrap_servers']}")
    print(f"  Tópico Escucha : {KAFKA_CONFIG['topic_lecturas']}")
    print(f"  Lake Processed : {LAKE_PROCESSED}/")
    print(f"  Data Warehouse : PostgreSQL — {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}")
    print("=" * 55)

    print("\n  Inicializando Estructuras en PostgreSQL...")
    try:
        inicializar_dwh()
        print("  ✓ DWH e índices listos para operar.")
    except Exception as e:
        print(f"\n  ✗ Error de conexión con PostgreSQL: {e}")
        exit(1)

    # Inicializar el Consumidor de Kafka
    try:
        consumer = KafkaConsumer(
            KAFKA_CONFIG["topic_lecturas"],
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            # 'earliest' asegura que lea todo lo que ya acumuló el simulador
            auto_offset_reset='earliest', 
            enable_auto_commit=True,
            # Cambiamos el grupo a 'v2' para reiniciar la sesión en el broker
            group_id='energytrack-pipeline-group-v2' 
        )
        print("  ✓ Conexión establecida con Kafka. Escuchando eventos...")
    except Exception as e:
        print(f"  ✗ Error crítico al conectar con Kafka: {e}")
        exit(1)

    num_ciclo = 0
    buffer_eventos = []
    ultimo_flush = time.time()
    
    print("\n  Esperando ráfagas de datos en tiempo real... Ctrl+C para apagar.")
    try:
        # Bucle de escucha perenne (Streaming de eventos reactivos)
        for mensaje in consumer:
            buffer_eventos.append(mensaje.value)
            
            # Para optimizar el rendimiento de inserción y permitir que el cálculo matemático de picos (3-sigma) 
            # funcione con un contexto real, procesamos los datos cuando se junta la ráfaga de los 8 medidores 
            # o cada 5 segundos como máximo. Esto garantiza tiempo real con altísima eficiencia.
            ahora = time.time()
            if len(buffer_eventos) >= 8 or (ahora - ultimo_flush) >= 5:
                num_ciclo += 1
                ejecutar_ciclo(buffer_eventos, num_ciclo)
                buffer_eventos.clear()
                ultimo_flush = ahora
                
    except KeyboardInterrupt:
        print("\n\n  [Sistema] Apagando el pipeline de streaming de forma segura...")
        if buffer_eventos:
            print("  [Sistema] Procesando últimos eventos residuales del buffer...")
            num_ciclo += 1
            ejecutar_ciclo(buffer_eventos, num_ciclo)
        consumer.close()
        print("  [Sistema] Pipeline cerrado correctamente.\n")