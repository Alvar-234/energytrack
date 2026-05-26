"""
EnergyTrack — Simulador de Streaming (Versión Kafka / Arquitectura Kappa)
========================================================================
Simula medidores inteligentes emitiendo eventos directos en tiempo real.

EJECUTAR: python simulador_streaming.py
"""

import random
import time
import json
import numpy as np
from datetime import datetime
from kafka import KafkaProducer
from config import KAFKA_CONFIG, MEDIDORES, INTERVALO_LECTURA_S

# ══════════════════════════════════════════════════════════
#  PERFIL DE POTENCIA REALISTA (Watts)
# ══════════════════════════════════════════════════════════

def watts_base(hora: int) -> tuple[float, float]:
    """Devuelve (media_watts, std_watts) para la hora dada."""
    if   0  <= hora <  6:   return  80,  30   # Madrugada: mínimo
    elif 6  <= hora <  9:   return 600, 200   # Mañana: ducha, desayuno
    elif 9  <= hora < 13:   return 300, 100   # Media mañana: trabajo
    elif 13 <= hora < 15:   return 700, 250   # Mediodía: comida
    elif 15 <= hora < 18:   return 250,  80   # Tarde: actividad baja
    elif 18 <= hora < 22:   return 900, 350   # Noche: TV, AC, cocina
    else:                   return 200,  60   # Noche tardía

CAUSAS_NULA = [
    "timeout_comunicacion",
    "señal_debil",
    "bateria_baja",
    "reinicio_medidor",
    "corte_temporal"
]

PROB_NULA = 0.05

def generar_lectura_nula(med_id, hog_id, ciudad, tarifa_clave) -> dict:
    """Simula una lectura fallida para enviar como evento crudo a Kafka."""
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

def generar_lectura(med_id, hog_id, ciudad, tarifa_clave, factor) -> dict:
    """Genera una lectura eléctrica correcta."""
    hora = datetime.now().hour
    media_w, std_w = watts_base(hora)
    watts = max(10.0, np.random.normal(media_w * factor, std_w * factor))

    es_pico = random.random() < 0.03
    if es_pico:
        watts *= random.uniform(2.5, 4.0)

    kwh = watts * (INTERVALO_LECTURA_S / 3600) / 1000
    voltaje         = round(np.random.normal(127, 3), 1)
    corriente       = round(watts / voltaje, 3)
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
#  PUNTO DE ENTRADA PRINCIPAL
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Mostrar pantalla de inicio
    print("=" * 65)
    print("  EnergyTrack — Simulador de Streaming (PRODUCTOR KAFKA)")
    print("=" * 65)
    print(f"  Medidores activos: {len(MEDIDORES)}")
    print(f"  Frecuencia envío : Cada {INTERVALO_LECTURA_S}s")
    print(f"  Tópico Kafka     : {KAFKA_CONFIG['topic_lecturas']}")
    print("=" * 65)
    
    # 1. Inicializar el Productor de Kafka local
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            # Convierte automáticamente los diccionarios Python a formato JSON listo para la red
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("  ✓ Conexión exitosa con el Broker de Kafka.")
    except Exception as e:
        print(f"  ✗ Error crítico al conectar con Kafka: {e}")
        print("  Asegúrate de haber ejecutado 'docker-compose up -d' primero.")
        exit(1)

    print("  Ctrl+C para detener la simulación")
    print("=" * 65)

    ciclo = 0
    try:
        while True:
            ciclo += 1
            ts = datetime.now().strftime("%H:%M:%S")
            print(f"\n  [{ts}] Disparando ráfaga de eventos — Ciclo #{ciclo}")

            for med_id, hog_id, ciudad, tarifa_clave, factor in MEDIDORES:
                # Inyección aleatoria de datos fallidos (Calidad de datos)
                if random.random() < PROB_NULA:
                    lectura = generar_lectura_nula(med_id, hog_id, ciudad, tarifa_clave)
                    print(f"    ✗ EVENTO NULO  | {med_id} | {ciudad:<16} | Causa: {lectura['estado']}")
                else:
                    lectura = generar_lectura(med_id, hog_id, ciudad, tarifa_clave, factor)
                    estado = "⚠ PICO" if lectura["es_pico"] else "  ok  "
                    print(f"    {estado} ENVIADO | {med_id} | {ciudad:<16} | "
                          f"{lectura['potencia_w']:>7.1f} W | {lectura['kwh_intervalo']:.6f} kWh")

                # 2. ENVIAR EL EVENTO INDIVIDUAL EN TIEMPO REAL A KAFKA
                producer.send(KAFKA_CONFIG["topic_lecturas"], value=lectura)

            # Forzar a Kafka a limpiar su buffer de red interno y asegurar el envío de la ráfaga
            producer.flush()
            
            # Dormir los 3 segundos establecidos antes del siguiente ciclo de eventos
            time.sleep(INTERVALO_LECTURA_S)

    except KeyboardInterrupt:
        print("\n\n  [Sistema] Deteniendo el simulador...")
        producer.close()
        print("  [Sistema] Conexiones con Kafka cerradas. Simulador apagado.\n")