"""
EnergyTrack — Módulo de conexión a base de datos
Centraliza la conexión al Data Warehouse (PostgreSQL).
El stream buffer sigue en SQLite porque son datos temporales.
"""

import psycopg2
import psycopg2.extras
import pandas as pd
from sqlalchemy import create_engine
from config import PG_CONFIG

# 1. MOTOR SQLALCHEMY (Exclusivo para lectura optimizada con Pandas)
DATABASE_URL = f"postgresql://{PG_CONFIG['user']}:{PG_CONFIG['password']}@{PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}"
engine = create_engine(DATABASE_URL)


# 2. CONEXIÓN DIRECTA (Exclusiva para escritura masiva en el pipeline ETL)
def get_dwh_conn():
    """Retorna una conexión activa al Data Warehouse en PostgreSQL."""
    return psycopg2.connect(**PG_CONFIG)


# 3. FUNCIONES DE OPERACIÓN
def dwh_query(sql: str, params=None) -> pd.DataFrame:
    """
    Ejecuta una query en el DWH y retorna un DataFrame.
    Usa el motor SQLAlchemy para eliminar la advertencia de Pandas.
    """
    try:
        return pd.read_sql(sql, engine, params=params)
    except Exception as e:
        print(f"  [DWH ERROR] {e}")
        return pd.DataFrame()


def dwh_execute(sql: str, params=None, many=False, data=None):
    """
    Ejecuta una operación de escritura en el DWH.
    Para inserciones masivas usa many=True con data=[lista de tuplas].
    """
    conn = get_dwh_conn()
    cur  = conn.cursor()
    try:
        if many and data:
            psycopg2.extras.execute_batch(cur, sql, data, page_size=500)
        else:
            cur.execute(sql, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


def dwh_execute_script(statements: list[str]):
    """Ejecuta múltiples sentencias DDL en secuencia."""
    conn = get_dwh_conn()
    cur  = conn.cursor()
    try:
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()