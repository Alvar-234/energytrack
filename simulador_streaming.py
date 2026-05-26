"""
EnergyTrack — Simulador de Streaming
=====================================
Simula medidores inteligentes con valores realistas para México.

Consumo objetivo por hogar promedio (factor=1.0):
  - Mensual : ~250 kWh/mes
  - Diario  : ~8.3 kWh/día
  - Horario : ~0.35 kWh/hora
  - Por lectura (3s): ~0.000292 kWh  (potencia ~350 W promedio)

El consumo se modela en Watts y se convierte a kWh por intervalo:
  kWh = Watts × (segundos / 3600)

EJECUTAR: python simulador_streaming.py
"""

import random, time, threading
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from config import (LAKE_RAW, MEDIDORES, TARIFAS_CFE,
                    INTERVALO_LECTURA_S, INTERVALO_LAKE_S)

# ══════════════════════════════════════════════════════════
#  PERFIL DE POTENCIA REALISTA (Watts)
#  Basado en consumo promedio mexicano ~250 kWh/mes
# ══════════════════════════════════════════════════════════

def watts_base(hora: int) -> tuple[float, float]:
    """
    Devuelve (media_watts, std_watts) para la hora dada.
    Un hogar promedio mexicano usa ~350 W promedio,
    con picos nocturnos de hasta 1,500-2,000 W (cocina, AC, TV).
    """
    if   0  <= hora <  6:   return  80,  30   # Madrugada: mínimo (focos, standby)
    elif 6  <= hora <  9:   return 600, 200   # Mañana: ducha, desayuno, plancha
    elif 9  <= hora < 13:   return 300, 100   # Media mañana: trabajo/escuela
    elif 13 <= hora < 15:   return 700, 250   # Mediodía: comida, microondas
    elif 15 <= hora < 18:   return 250,  80   # Tarde: actividad baja
    elif 18 <= hora < 22:   return 900, 350   # Noche: TV, AC, cocina, lavadora
    else:                   return 200,  60   # Noche tardía: bajando

# Causas reales de lecturas nulas en medidores inteligentes
CAUSAS_NULA = [
    "timeout_comunicacion",  # el medidor no respondió a tiempo
    "señal_debil",           # interferencia en la transmisión
    "bateria_baja",          # medidor con batería baja
    "reinicio_medidor",      # medidor reiniciando firmware
    "corte_temporal",        # micro-corte de suministro (<1s)
]

# Probabilidad de lectura nula por ciclo (≈5% = 1 de cada 20 lecturas)
PROB_NULA = 0.05

def generar_lectura_nula(med_id, hog_id, ciudad, tarifa_clave) -> dict:
    """
    Simula una lectura fallida.
    SE GUARDA en el Data Lake (auditoría histórica).
    El ETL la detecta por estado != 'ok' y kwh=None → nunca llega al DWH.
    """
    return {
        "timestamp":       datetime.now().isoformat(timespec="seconds"),
        "id_medidor":      med_id,
        "id_hogar":        hog_id,
        "ciudad":          ciudad,
        "tarifa":          tarifa_clave,
        "kwh_intervalo":   None,
        "potencia_w":      None,
        "voltaje":         None,
        "corriente":       None,
        "factor_potencia": None,
        "es_pico":         0,
        "estado":          random.choice(CAUSAS_NULA),
    }

def generar_lectura(med_id, hog_id, ciudad, tarifa_clave, factor):
    hora = datetime.now().hour

    # Potencia en Watts para este instante
    media_w, std_w = watts_base(hora)
    watts = max(10.0, np.random.normal(media_w * factor, std_w * factor))

    # Pico de consumo (AC encendiendo, horno, etc.)
    es_pico = random.random() < 0.03
    if es_pico:
        watts *= random.uniform(2.5, 4.0)   # 2.5x-4x el consumo normal

    # Convertir a kWh para este intervalo de 3 segundos
    kwh = watts * (INTERVALO_LECTURA_S / 3600) / 1000

    # Valores eléctricos derivados (red doméstica México: 127V)
    voltaje         = round(np.random.normal(127, 3), 1)
    corriente       = round(watts / voltaje, 3)           # I = P/V
    factor_potencia = round(random.uniform(0.85, 0.99), 3)
    potencia_w      = round(watts, 2)

    return {
        "timestamp":       datetime.now().isoformat(timespec="seconds"),
        "id_medidor":      med_id,
        "id_hogar":        hog_id,
        "ciudad":          ciudad,
        "tarifa":          tarifa_clave,
        "kwh_intervalo":   round(kwh, 6),
        "potencia_w":      potencia_w,
        "voltaje":         voltaje,
        "corriente":       corriente,
        "factor_potencia": factor_potencia,
        "es_pico":         int(es_pico),
        "estado":          "ok",
    }

# ══════════════════════════════════════════════════════════
#  HILO 1 — PRODUCTOR
# ══════════════════════════════════════════════════════════

def hilo_productor(buffer, lock, stop):
    ciclo = 0
    while not stop.is_set():
        ciclo += 1
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"\n  [{ts}] Ciclo #{ciclo}")

        nuevas = []
        for med_id, hog_id, ciudad, tarifa_clave, factor in MEDIDORES:
            # ~5% de probabilidad de lectura nula (va al lake, nunca al DWH)
            if random.random() < PROB_NULA:
                lectura = generar_lectura_nula(med_id, hog_id, ciudad, tarifa_clave)
                nuevas.append(lectura)
                causa = lectura["estado"]
                print(f"    ✗ NULA | {med_id} | {ciudad:<20} | causa: {causa}")
            else:
                lectura = generar_lectura(med_id, hog_id, ciudad, tarifa_clave, factor)
                nuevas.append(lectura)
                estado = "⚠ PICO" if lectura["es_pico"] else "  ok  "
                print(f"    {estado} | {med_id} | {ciudad:<20} | "
                      f"{lectura['potencia_w']:>7.1f} W | {lectura['kwh_intervalo']:.6f} kWh")

        with lock:
            buffer.extend(nuevas)

        stop.wait(INTERVALO_LECTURA_S)

# ══════════════════════════════════════════════════════════
#  HILO 2 — ESCRITOR AL DATA LAKE
# ══════════════════════════════════════════════════════════

def hilo_lake_writer(buffer, lock, stop):
    while not stop.is_set():
        stop.wait(INTERVALO_LAKE_S)
        if stop.is_set():
            break
        vaciar_buffer(buffer, lock)

def vaciar_buffer(buffer, lock):
    with lock:
        if not buffer:
            return
        lote = buffer.copy()
        buffer.clear()

    df = pd.DataFrame(lote)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["lote"] = df["timestamp"].dt.strftime("%Y-%m-%d_%H")

    archivos = []
    for lote_key, grupo in df.groupby("lote"):
        ruta = LAKE_RAW / lote_key
        ruta.mkdir(parents=True, exist_ok=True)
        ts_str  = datetime.now().strftime("%H%M%S")
        archivo = ruta / f"stream_{ts_str}.parquet"
        grupo.drop(columns=["lote"]).to_parquet(archivo, index=False)
        archivos.append((archivo, len(grupo)))

    ts = datetime.now().strftime("%H:%M:%S")
    total = sum(n for _, n in archivos)
    print(f"\n  [{ts}] 💾 DATA LAKE ← {total} lecturas")
    for archivo, n in archivos:
        kb = archivo.stat().st_size / 1024
        print(f"    → {archivo}  ({n} filas, {kb:.1f} KB)")

# ══════════════════════════════════════════════════════════
#  PUNTO DE ENTRADA
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    LAKE_RAW.mkdir(parents=True, exist_ok=True)

    # Mostrar resumen de tarifas activas
    print("=" * 65)
    print("  EnergyTrack — Simulador de Streaming")
    print("=" * 65)
    print(f"  Medidores    : {len(MEDIDORES)}")
    print(f"  Lectura cada : {INTERVALO_LECTURA_S}s")
    print(f"  Flush lake   : cada {INTERVALO_LAKE_S}s → {LAKE_RAW}/")
    print()
    print("  CONSUMO ESPERADO (factor=1.0 → ~250 kWh/mes):")
    print(f"  {'Medidor':<10} {'Ciudad':<20} {'Tarifa':<6} {'Factor':>6}  {'kWh/mes est.':>12}")
    print("  " + "-"*60)
    for med_id, hog_id, ciudad, tarifa_clave, factor in MEDIDORES:
        kwh_mes = round(250 * factor, 0)
        print(f"  {med_id:<10} {ciudad:<20} {tarifa_clave:<6} {factor:>6.1f}  {kwh_mes:>10.0f} kWh")
    print("=" * 65)
    print("  Ctrl+C para detener")
    print("=" * 65)

    buffer = []
    lock   = threading.Lock()
    stop   = threading.Event()

    t1 = threading.Thread(target=hilo_productor,   args=(buffer, lock, stop), daemon=True)
    t2 = threading.Thread(target=hilo_lake_writer, args=(buffer, lock, stop), daemon=True)
    t1.start()
    t2.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n  Deteniendo...")
        stop.set()
        vaciar_buffer(buffer, lock)
        t1.join(timeout=5)
        t2.join(timeout=5)
        print("  Simulador detenido.\n")