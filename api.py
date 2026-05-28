"""
EnergyTrack — API REST (Flask)
Sirve datos al frontend HTML/JS y genera reportes PDF.
Correr: python api.py
"""

import io, base64
from datetime import datetime, timedelta
from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                Table, TableStyle, HRFlowable)
from db import dwh_query

app = Flask(__name__)
CORS(app)

# helpers

def fmt_kwh(v):
    v = float(v or 0)
    if v >= 1000:  return f"{v:,.1f}"
    elif v >= 1:   return f"{v:.3f}"
    elif v >= 0.001: return f"{v:.4f}"
    else:          return f"{v:.6f}"

def fmt_mxn(v):
    v = float(v or 0)
    if v >= 100:   return f"${v:,.2f}"
    elif v >= 1:   return f"${v:.3f}"
    elif v >= 0.001: return f"${v:.4f}"
    else:          return f"${v:.6f}"

def _where_periodo(periodo: str, valor: str) -> str:
    valor = str(valor).strip()
    if not valor:
        return "1=1"
    try:
        if periodo == "hora" and " " in valor:
            fecha, hora = valor.rsplit(" ", 1)
            return f"t.fecha = '{fecha}' AND t.hora = {int(hora)}"
        elif periodo == "dia":
            if len(valor.split(" ")[0]) >= 10:
                return f"t.fecha = '{valor.split(' ')[0]}'"
        elif periodo == "semana" and "-W" in valor:
            year, week = valor.split("-W")
            return f"t.year = {year} AND EXTRACT(WEEK FROM t.fecha::date) = {int(week)}"
        elif periodo == "mes" and "-" in valor and "-W" not in valor:
            parts = valor.split("-")
            if len(parts) == 2:
                return f"t.year = {parts[0]} AND t.month = {int(parts[1])}"
    except Exception:
        pass
    return "1=1"

def _label_periodo(periodo: str, valor: str) -> str:
    meses = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
             7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
    valor = str(valor)
    try:
        if periodo == "hora" and " " in valor:
            fecha, hora = valor.rsplit(" ", 1)
            return f"{fecha}  {int(hora):02d}:00 – {int(hora):02d}:59 h"
        elif periodo == "dia":
            d = datetime.strptime(valor.split(" ")[0], "%Y-%m-%d")
            return f"{d.day} de {meses[d.month]} de {d.year}"
        elif periodo == "semana" and "-W" in valor:
            y, w = valor.split("-W")
            lunes = datetime.strptime(f"{y}-W{int(w)}-1", "%Y-W%W-%w")
            domingo = lunes + timedelta(days=6)
            return f"Semana {w} · {lunes.strftime('%d/%m')} – {domingo.strftime('%d/%m/%Y')}"
        elif periodo == "mes" and "-" in valor:
            y, m = valor.split("-")
            return f"{meses[int(m)]} {y}"
    except Exception:
        pass
    return valor

# ndpoints de datos

@app.route("/api/resumen")
def api_resumen():
    df = dwh_query("SELECT * FROM v_resumen_general")
    if df.empty:
        return jsonify({"ok": False})
    r = df.iloc[0]
    return jsonify({
        "ok": True,
        "total_lecturas": int(r["total_lecturas"] or 0),
        "kwh_total": float(r["kwh_total"] or 0),
        "kwh_total_fmt": fmt_kwh(r["kwh_total"]),
        "costo_total": float(r["costo_total"] or 0),
        "costo_total_fmt": fmt_mxn(r["costo_total"]),
        "total_picos": int(r["total_picos"] or 0),
        "hogares_activos": int(r["hogares_activos"] or 0),
    })

@app.route("/api/consumo-region")
def api_consumo_region():
    df = dwh_query("SELECT region, SUM(kwh_total) AS kwh, SUM(costo_total) AS costo FROM v_consumo_por_region GROUP BY region ORDER BY kwh DESC")
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/ranking-hogares")
def api_ranking_hogares():
    df = dwh_query("SELECT id_hogar, ciudad, kwh_total AS kwh FROM v_consumo_por_hogar ORDER BY kwh ASC")
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/serie-diaria")
def api_serie_diaria():
    df = dwh_query("""
        SELECT fecha, region, ROUND(SUM(kwh_total)::numeric, 3) AS kwh
        FROM v_consumo_por_region GROUP BY fecha, region ORDER BY fecha
    """)
    df["fecha"] = df["fecha"].astype(str)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/perfil-horario")
def api_perfil_horario():
    df = dwh_query("SELECT hora, ROUND(kwh_promedio::numeric,5) AS prom, ROUND(kwh_max::numeric,4) AS maximo FROM v_perfil_horario ORDER BY hora")
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/heatmap")
def api_heatmap():
    df = dwh_query("""
        SELECT t.dia_semana, t.hora, AVG(f.kwh_intervalo) AS kwh
        FROM fact_consumo f JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        GROUP BY t.dia_semana, t.hora
    """)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/picos")
def api_picos():
    df = dwh_query("""
        SELECT f.timestamp_real AS timestamp, r.region, f.kwh_intervalo
        FROM fact_consumo f
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE f.es_pico = true
        LIMIT 500
    """)
    df["timestamp"] = df["timestamp"].astype(str)
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/picos-por-hora")
def api_picos_por_hora():
    df = dwh_query("SELECT hora, num_picos FROM v_perfil_horario ORDER BY hora")
    todas = {h: 0 for h in range(24)}
    for _, r in df.iterrows():
        todas[int(r["hora"])] = int(r["num_picos"])
    return jsonify([{"hora": h, "num_picos": todas[h]} for h in range(24)])

@app.route("/api/top-picos")
def api_top_picos():
    df = dwh_query("""
        SELECT f.timestamp_real AS timestamp, h.ciudad, r.region,
               ROUND(f.kwh_intervalo::numeric,4) AS kwh,
               ROUND(f.costo_mxn::numeric,4) AS costo
        FROM fact_consumo f
        JOIN dim_hogar h ON f.hogar_key = h.hogar_key
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE f.es_pico = true
        ORDER BY f.kwh_intervalo DESC
        LIMIT 10
    """)
    df["timestamp"] = df["timestamp"].astype(str)
    return jsonify(df.to_dict(orient="records"))

# endpoints de periodos

@app.route("/api/periodos")
def api_periodos():
    tipo = request.args.get("tipo", "dia")
    try:
        if tipo == "hora":
            df = dwh_query("SELECT DISTINCT fecha||' '||hora AS fh FROM dim_tiempo ORDER BY fh DESC LIMIT 72")
            items = []
            for fh in df["fh"].tolist():
                if fh:
                    fp, hp = str(fh).split(" ")
                    items.append({"label": f"{hp.zfill(2)}:00 h — {fp}", "value": str(fh)})
        elif tipo == "dia":
            df = dwh_query("SELECT DISTINCT fecha FROM dim_tiempo ORDER BY fecha DESC LIMIT 90")
            items = [{"label": str(o), "value": str(o)} for o in df["fecha"].tolist() if o]
        elif tipo == "semana":
            df = dwh_query("SELECT DISTINCT year||'-W'||LPAD(EXTRACT(WEEK FROM fecha::date)::text,2,'0') AS sem FROM dim_tiempo ORDER BY sem DESC")
            items = [{"label": str(o), "value": str(o)} for o in df["sem"].tolist() if o]
        else:
            df = dwh_query("SELECT DISTINCT year||'-'||LPAD(month::text,2,'0') AS mes FROM dim_tiempo ORDER BY mes DESC")
            items = [{"label": str(o), "value": str(o)} for o in df["mes"].tolist() if o]
    except Exception:
        items = []
    return jsonify(items)

@app.route("/api/preview")
def api_preview():
    tipo = request.args.get("tipo", "dia")
    valor = request.args.get("valor", "")
    if not valor:
        return jsonify({"ok": False, "error": "No value"})
    where = _where_periodo(tipo, valor)
    label = _label_periodo(tipo, valor)
    r = dwh_query(f"""
        SELECT COUNT(*) AS lecturas,
               ROUND(SUM(f.kwh_intervalo)::numeric,6) AS kwh,
               ROUND(SUM(f.costo_mxn)::numeric,4)     AS costo,
               SUM(f.es_pico::int)                    AS picos,
               COUNT(DISTINCT f.hogar_key)            AS hogares
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        WHERE {where}
    """)
    if r.empty or r.iloc[0]["lecturas"] == 0:
        return jsonify({"ok": False, "label": label})
    row = r.iloc[0]
    reg = dwh_query(f"""
        SELECT r.region, ROUND(SUM(f.kwh_intervalo)::numeric,6) AS kwh
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE {where}
        GROUP BY r.region ORDER BY kwh DESC
    """)
    return jsonify({
        "ok": True,
        "label": label,
        "lecturas": int(row["lecturas"]),
        "kwh": float(row["kwh"]),
        "kwh_fmt": fmt_kwh(row["kwh"]),
        "costo": float(row["costo"]),
        "costo_fmt": fmt_mxn(row["costo"]),
        "picos": int(row["picos"] or 0),
        "hogares": int(row["hogares"]),
        "regiones": reg.to_dict(orient="records"),
    })

# PDF

AZUL_CFE   = colors.HexColor("#1E3A5F")
CYAN       = colors.HexColor("#38BDF8")
VERDE_PDF  = colors.HexColor("#10B981")
ROJO_PDF   = colors.HexColor("#EF4444")
GRIS       = colors.HexColor("#94A3B8")

@app.route("/api/reporte-pdf")
def api_reporte_pdf():
    tipo = request.args.get("tipo", "dia")
    valor = request.args.get("valor", "")
    if not valor:
        return jsonify({"ok": False, "error": "No value"}), 400
    where = _where_periodo(tipo, valor)
    label = _label_periodo(tipo, valor)
    resumen = dwh_query(f"""
        SELECT COUNT(*) AS lecturas,
               ROUND(SUM(f.kwh_intervalo)::numeric, 6) AS kwh_total,
               ROUND(SUM(f.costo_mxn)::numeric, 4)     AS costo_total,
               SUM(f.es_pico::int)                     AS total_picos,
               COUNT(DISTINCT f.hogar_key)             AS hogares
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        WHERE {where}
    """)
    por_region = dwh_query(f"""
        SELECT r.region,
               ROUND(SUM(f.kwh_intervalo)::numeric, 6) AS kwh,
               ROUND(SUM(f.costo_mxn)::numeric, 4)     AS costo,
               SUM(f.es_pico::int)                     AS picos,
               COUNT(*)                                AS lecturas
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE {where}
        GROUP BY r.region ORDER BY kwh DESC
    """)
    por_hogar = dwh_query(f"""
        SELECT h.id_hogar, h.ciudad, r.region,
               ROUND(SUM(f.kwh_intervalo)::numeric, 6) AS kwh,
               ROUND(SUM(f.costo_mxn)::numeric, 4)     AS costo,
               SUM(f.es_pico::int)                     AS picos
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        JOIN dim_hogar  h ON f.hogar_key  = h.hogar_key
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE {where}
        GROUP BY h.id_hogar, h.ciudad, r.region ORDER BY kwh DESC LIMIT 8
    """)
    picos_top = dwh_query(f"""
        SELECT f.timestamp_real, h.id_hogar, h.ciudad,
               ROUND(f.kwh_intervalo::numeric, 6) AS kwh,
               ROUND(f.costo_mxn::numeric, 4)     AS costo
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        JOIN dim_hogar  h ON f.hogar_key  = h.hogar_key
        WHERE f.es_pico = true AND {where}
        ORDER BY f.kwh_intervalo DESC LIMIT 10
    """)
    perfil = dwh_query(f"""
        SELECT t.hora,
               ROUND(AVG(f.kwh_intervalo)::numeric, 6) AS kwh_prom,
               ROUND(MAX(f.kwh_intervalo)::numeric, 6) AS kwh_max
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        WHERE {where}
        GROUP BY t.hora ORDER BY t.hora
    """)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)
    st = getSampleStyleSheet()
    story = []
    s_titulo  = ParagraphStyle("titulo", fontSize=22, textColor=AZUL_CFE, fontName="Helvetica-Bold", spaceAfter=4, alignment=TA_LEFT)
    s_subtit  = ParagraphStyle("subtit", fontSize=12, textColor=GRIS, fontName="Helvetica", spaceAfter=2, alignment=TA_LEFT)
    s_seccion = ParagraphStyle("seccion", fontSize=11, textColor=AZUL_CFE, fontName="Helvetica-Bold", spaceBefore=14, spaceAfter=6)
    s_normal  = ParagraphStyle("normal", fontSize=9, textColor=colors.black, fontName="Helvetica", spaceAfter=4)
    s_footer  = ParagraphStyle("footer", fontSize=7, textColor=GRIS, fontName="Helvetica", alignment=TA_CENTER)
    def tabla(encabezados, filas, col_widths, col_colors=None):
        data = [encabezados] + filas
        t = Table(data, colWidths=col_widths, repeatRows=1)
        style = [
            ("BACKGROUND",   (0,0), (-1,0),  AZUL_CFE),
            ("TEXTCOLOR",    (0,0), (-1,0),  colors.white),
            ("FONTNAME",     (0,0), (-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",     (0,0), (-1,-1), 8),
            ("ALIGN",        (0,0), (-1,-1), "CENTER"),
            ("ALIGN",        (0,1), (1,-1),  "LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, colors.HexColor("#F8FAFC")]),
            ("GRID",         (0,0), (-1,-1), 0.3, colors.HexColor("#E2E8F0")),
            ("TOPPADDING",   (0,0), (-1,-1), 5),
            ("BOTTOMPADDING",(0,0), (-1,-1), 5),
            ("LEFTPADDING",  (0,0), (-1,-1), 6),
        ]
        if col_colors:
            for col_idx, color_val in col_colors:
                style.append(("TEXTCOLOR", (col_idx,1), (col_idx,-1), color_val))
        t.setStyle(TableStyle(style))
        return t
    story.append(Paragraph("⚡ EnergyTrack", s_titulo))
    story.append(Paragraph("Reporte de Consumo Energético", s_subtit))
    story.append(Paragraph(
        f"Periodo: <b>{label}</b>  ·  "
        f"Tipo: <b>{tipo.capitalize()}</b>  ·  "
        f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}", s_normal))
    story.append(HRFlowable(width="100%", thickness=2, color=AZUL_CFE, spaceAfter=10))
    if resumen.empty or resumen.iloc[0]["lecturas"] == 0:
        story.append(Paragraph("⚠ Sin datos para el periodo seleccionado.", s_normal))
        doc.build(story)
        pdf_bytes = buf.getvalue()
        return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf", as_attachment=True, download_name=f"EnergyTrack_{tipo}_{valor.replace(' ','_').replace(':','-')}.pdf")
    r = resumen.iloc[0]
    story.append(Paragraph("Resumen del Periodo", s_seccion))
    kpis_data = [
        ["Lecturas", "Consumo Total", "Costo Total", "Picos", "Hogares"],
        [str(int(r["lecturas"])), fmt_kwh(r["kwh_total"])+" kWh", fmt_mxn(r["costo_total"])+" MXN", str(int(r["total_picos"] or 0)), str(int(r["hogares"]))]
    ]
    kpi_t = Table(kpis_data, colWidths=[1.2*inch]*5)
    kpi_t.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,0),  AZUL_CFE),
        ("TEXTCOLOR",    (0,0), (-1,0),  colors.white),
        ("BACKGROUND",   (0,1), (0,1),   colors.HexColor("#EFF6FF")),
        ("BACKGROUND",   (1,1), (1,1),   colors.HexColor("#ECFDF5")),
        ("BACKGROUND",   (2,1), (2,1),   colors.HexColor("#FFFBEB")),
        ("BACKGROUND",   (3,1), (3,1),   colors.HexColor("#FEF2F2")),
        ("BACKGROUND",   (4,1), (4,1),   colors.HexColor("#F0F9FF")),
        ("FONTNAME",     (0,0), (-1,0),  "Helvetica-Bold"),
        ("FONTNAME",     (0,1), (-1,1),  "Helvetica-Bold"),
        ("FONTSIZE",     (0,0), (-1,-1), 9),
        ("ALIGN",        (0,0), (-1,-1), "CENTER"),
        ("GRID",         (0,0), (-1,-1), 0.5, colors.HexColor("#CBD5E1")),
        ("TOPPADDING",   (0,0), (-1,-1), 8),
        ("BOTTOMPADDING",(0,0), (-1,-1), 8),
        ("TEXTCOLOR",    (1,1), (1,1),   VERDE_PDF),
        ("TEXTCOLOR",    (3,1), (3,1),   ROJO_PDF),
    ]))
    story.append(kpi_t)
    story.append(Spacer(1, 10))
    if not por_region.empty:
        story.append(Paragraph("Consumo por Región  ·  Tarifa CFE", s_seccion))
        filas_r = [[row["region"], fmt_kwh(row["kwh"]), fmt_mxn(row["costo"]),
                    str(int(row["picos"] or 0)), str(int(row["lecturas"]))]
                   for _, row in por_region.iterrows()]
        story.append(tabla(
            ["Región / Tarifa", "Consumo (kWh)", "Costo (MXN)", "Picos", "Lecturas"],
            filas_r, [1.8*inch, 1.4*inch, 1.4*inch, 0.8*inch, 0.9*inch], col_colors=[(2, ROJO_PDF)]))
        story.append(Spacer(1, 6))
    if not por_hogar.empty:
        story.append(Paragraph("Ranking de Hogares por Consumo", s_seccion))
        filas_h = [[str(i+1), row["id_hogar"], row["ciudad"], row["region"],
                    fmt_kwh(row["kwh"]), fmt_mxn(row["costo"]), str(int(row["picos"] or 0))]
                   for i, (_, row) in enumerate(por_hogar.iterrows())]
        story.append(tabla(
            ["#", "Medidor", "Ciudad", "Región/Tarifa", "kWh", "Costo MXN", "Picos"],
            filas_h, [0.3*inch, 0.9*inch, 1.3*inch, 1.1*inch, 1.1*inch, 1.1*inch, 0.6*inch]))
        story.append(Spacer(1, 6))
    if not perfil.empty and len(perfil) > 1:
        story.append(Paragraph("Perfil de Consumo por Hora", s_seccion))
        filas_p = [[f"{int(row['hora']):02d}:00 h", fmt_kwh(row["kwh_prom"]), fmt_kwh(row["kwh_max"])]
                   for _, row in perfil.iterrows()]
        story.append(tabla(["Hora", "Promedio (kWh)", "Máximo (kWh)"], filas_p, [1.5*inch, 2*inch, 2*inch]))
        story.append(Spacer(1, 6))
    if not picos_top.empty:
        story.append(Paragraph(f"Anomalías Detectadas  ·  Top {len(picos_top)}", s_seccion))
        filas_pk = [[str(row["timestamp_real"])[:19], row["id_hogar"], row["ciudad"],
                     fmt_kwh(row["kwh"]), fmt_mxn(row["costo"])]
                    for _, row in picos_top.iterrows()]
        story.append(tabla(
            ["Timestamp", "Medidor", "Ciudad", "kWh (pico)", "Costo MXN"],
            filas_pk, [1.8*inch, 0.9*inch, 1.2*inch, 1.2*inch, 1.2*inch], col_colors=[(3, ROJO_PDF)]))
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.5, color=GRIS, spaceAfter=4))
    story.append(Paragraph(
        f"EnergyTrack · Proyecto Final UV 2026 · "
        f"Datos: Data Warehouse PostgreSQL · "
        f"Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}", s_footer))
    doc.build(story)
    pdf_bytes = buf.getvalue()
    nombre = f"EnergyTrack_{tipo}_{valor.replace(' ', '_').replace(':', '-').replace('/', '-')}.pdf"
    return send_file(io.BytesIO(pdf_bytes), mimetype="application/pdf", as_attachment=True, download_name=nombre)

if __name__ == "__main__":
    print("\n  ╔══════════════════════════════════════════════╗")
    print("  ║  EnergyTrack — API REST (Flask)              ║")
    print("  ║  http://127.0.0.1:5000/api/resumen           ║")
    print("  ╚══════════════════════════════════════════════╝\n")
    app.run(debug=False, host="127.0.0.1", port=5000)
