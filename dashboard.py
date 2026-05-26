"""
EnergyTrack — Dashboard (Versión Streaming / Arquitectura Kappa)
Lee ÚNICAMENTE del Data Warehouse (PostgreSQL).
No accede al lake ni al stream directamente.

EJECUTAR:
  python dashboard.py
  Abrir: http://127.0.0.1:8050
"""

import pandas as pd
from db import dwh_query as _dwh_q
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, dcc, html, Input, Output
import dash_bootstrap_components as dbc
import base64, io, json
from datetime import datetime, timedelta
from config import LAKE_RAW, LAKE_PROCESSED, PG_CONFIG

# PDF
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                Table, TableStyle, HRFlowable)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT

# ══════════════════════════════════════════════════════════
#  LEER DEL DWH
# ══════════════════════════════════════════════════════════

def dwh(sql: str) -> pd.DataFrame:
    """Consulta el Data Warehouse (PostgreSQL) and devuelve un DataFrame."""
    return _dwh_q(sql)

def estado_sistema() -> dict:
    """KPIs generales del sistema para el sidebar."""
    try:
        df = dwh("SELECT * FROM v_resumen_general")
        if df.empty:
            return {"dwh_ok": False}
        row = df.iloc[0]
        return {
            "total_lecturas":  int(row["total_lecturas"] or 0),
            "kwh_total":       float(row["kwh_total"] or 0),
            "costo_total":     float(row["costo_total"] or 0),
            "total_picos":     int(row["total_picos"] or 0),
            "hogares_activos": int(row["hogares_activos"] or 0),
            "dwh_ok":          True,
        }
    except Exception as e:
        return {"dwh_ok": False}

# ══════════════════════════════════════════════════════════
#  TEMA VISUAL
# ══════════════════════════════════════════════════════════

BG      = "#0F172A"
CARD    = "#1E293B"
BORDE   = "#334155"
TEXTO   = "#F1F5F9"
SUBTXT  = "#94A3B8"
ACCENT  = "#38BDF8"
VERDE   = "#10B981"
ROJO    = "#EF4444"
AMBAR   = "#F59E0B"
PALETA  = {"Norte":ROJO,"Centro":ACCENT,"Occidente":VERDE,"Sureste":AMBAR}

# Formato numérico adaptativo
def fmt_kwh(v):
    """Muestra los decimales necesarios según la magnitud."""
    v = float(v or 0)
    if   v >= 1000:  return f"{v:,.1f}"
    elif v >= 1:     return f"{v:.3f}"
    elif v >= 0.001: return f"{v:.4f}"
    else:            return f"{v:.6f}"

def fmt_mxn(v):
    v = float(v or 0)
    if   v >= 100:   return f"${v:,.2f}"
    elif v >= 1:     return f"${v:.3f}"
    elif v >= 0.001: return f"${v:.4f}"
    else:            return f"${v:.6f}"

def fmt_bar(v):
    """Etiqueta para barras: usa notación científica si es muy pequeño."""
    v = float(v or 0)
    if   v >= 1:     return f"{v:.2f}"
    elif v >= 0.001: return f"{v:.4f}"
    elif v > 0:      return f"{v:.2e}"
    else:            return "0"

def card(titulo, contenido, borde=ACCENT):
    return html.Div(style={
        "background":CARD,"borderRadius":"12px","padding":"18px","height":"100%",
        "border":f"1px solid {BORDE}","borderTop":f"3px solid {borde}",
    }, children=[
        html.P(titulo, style={"color":SUBTXT,"fontSize":"10px","textTransform":"uppercase",
                               "letterSpacing":"1.5px","marginBottom":"10px","fontWeight":"700"}),
        contenido,
    ])

def kpi(titulo, valor, unidad="", borde=ACCENT):
    return dbc.Col(html.Div(style={
        "background":CARD,"borderRadius":"10px","padding":"16px 20px",
        "border":f"1px solid {BORDE}","borderLeft":f"4px solid {borde}",
    }, children=[
        html.P(titulo, style={"color":SUBTXT,"fontSize":"10px",
                               "textTransform":"uppercase","margin":"0 0 6px","letterSpacing":"1px"}),
        html.Span(str(valor), style={"fontSize":"28px","fontWeight":"700","color":TEXTO}),
        html.Span(f" {unidad}", style={"fontSize":"13px","color":SUBTXT}),
    ]))

def fig(f, h=300):
    f.update_layout(
        height=h, margin=dict(l=12,r=12,t=24,b=12),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TEXTO,size=12),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        xaxis=dict(gridcolor=BORDE,linecolor=BORDE),
        yaxis=dict(gridcolor=BORDE,linecolor=BORDE),
    )
    return f

# ══════════════════════════════════════════════════════════
#  GENERADOR DE REPORTES PDF
# ══════════════════════════════════════════════════════════

AZUL_CFE  = colors.HexColor("#1E3A5F")
CYAN      = colors.HexColor("#38BDF8")
VERDE_PDF = colors.HexColor("#10B981")
ROJO_PDF  = colors.HexColor("#EF4444")
GRIS      = colors.HexColor("#94A3B8")
GRIS_SUAVE= colors.HexColor("#1E293B")

def _where_periodo(periodo: str, valor: str) -> str:
    """Genera cláusula WHERE de forma segura."""
    valor = str(valor)
    if periodo == "hora" and " " in valor:
        fecha, hora = valor.rsplit(" ", 1)
        return f"t.fecha = '{fecha}' AND t.hora = {int(hora)}"
    elif periodo == "dia":
        return f"t.fecha = '{valor}'"
    elif periodo == "semana" and "-W" in valor:
        year, week = valor.split("-W")
        return f"t.year = {year} AND EXTRACT(WEEK FROM t.fecha::date) = {int(week)}"
    elif periodo == "mes" and "-" in valor:
        year, month = valor.split("-")
        return f"t.year = {year} AND t.month = {int(month)}"
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
            year, week = valor.split("-W")
            lunes = datetime.strptime(f"{year}-W{int(week)}-1", "%Y-W%W-%w")
            domingo = lunes + timedelta(days=6)
            return f"Semana {week} · {lunes.strftime('%d/%m')} – {domingo.strftime('%d/%m/%Y')}"
        elif periodo == "mes" and "-" in valor:
            year, month = valor.split("-")
            return f"{meses[int(month)]} {year}"
    except Exception:
        pass
    return valor

def generar_pdf(periodo: str, valor: str) -> bytes:
    """Genera el reporte PDF y devuelve los bytes."""
    where = _where_periodo(periodo, valor)
    label = _label_periodo(periodo, valor)

    resumen = dwh(f"""
        SELECT COUNT(*) AS lecturas,
               ROUND(SUM(f.kwh_intervalo)::numeric, 6) AS kwh_total,
               ROUND(SUM(f.costo_mxn)::numeric, 4)     AS costo_total,
               SUM(f.es_pico::int)                     AS total_picos,
               COUNT(DISTINCT f.hogar_key)             AS hogares
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        WHERE {where}
    """)

    por_region = dwh(f"""
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

    por_hogar = dwh(f"""
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

    picos_top = dwh(f"""
        SELECT f.timestamp_real, h.id_hogar, h.ciudad,
               ROUND(f.kwh_intervalo::numeric, 6) AS kwh,
               ROUND(f.costo_mxn::numeric, 4)     AS costo
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        JOIN dim_hogar  h ON f.hogar_key  = h.hogar_key
        WHERE f.es_pico = true AND {where}
        ORDER BY f.kwh_intervalo DESC LIMIT 10
    """)

    perfil = dwh(f"""
        SELECT t.hora,
               ROUND(AVG(f.kwh_intervalo)::numeric, 6) AS kwh_prom,
               ROUND(MAX(f.kwh_intervalo)::numeric, 6) AS kwh_max
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        WHERE {where}
        GROUP BY t.hora ORDER BY t.hora
    """)

    # Construir PDF
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=letter,
                            leftMargin=0.75*inch, rightMargin=0.75*inch,
                            topMargin=0.75*inch, bottomMargin=0.75*inch)
    st  = getSampleStyleSheet()
    story = []

    s_titulo   = ParagraphStyle("titulo",   fontSize=22, textColor=AZUL_CFE, fontName="Helvetica-Bold", spaceAfter=4, alignment=TA_LEFT)
    s_subtit   = ParagraphStyle("subtit",   fontSize=12, textColor=GRIS, fontName="Helvetica", spaceAfter=2, alignment=TA_LEFT)
    s_seccion  = ParagraphStyle("seccion",  fontSize=11, textColor=AZUL_CFE, fontName="Helvetica-Bold", spaceBefore=14, spaceAfter=6)
    s_normal   = ParagraphStyle("normal",   fontSize=9,  textColor=colors.black, fontName="Helvetica", spaceAfter=4)
    s_footer   = ParagraphStyle("footer",   fontSize=7,  textColor=GRIS, fontName="Helvetica", alignment=TA_CENTER)

    def tabla(encabezados, filas, col_widths, col_colors=None):
        data = [encabezados] + filas
        t = Table(data, colWidths=col_widths, repeatRows=1)
        style = [
            ("BACKGROUND",  (0,0), (-1,0),  AZUL_CFE),
            ("TEXTCOLOR",   (0,0), (-1,0),  colors.white),
            ("FONTNAME",    (0,0), (-1,0),  "Helvetica-Bold"),
            ("FONTSIZE",    (0,0), (-1,-1), 8),
            ("ALIGN",       (0,0), (-1,-1), "CENTER"),
            ("ALIGN",       (0,1), (1,-1),  "LEFT"),
            ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, colors.HexColor("#F8FAFC")]),
            ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#E2E8F0")),
            ("TOPPADDING",  (0,0), (-1,-1), 5),
            ("BOTTOMPADDING",(0,0), (-1,-1), 5),
            ("LEFTPADDING", (0,0), (-1,-1), 6),
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
        f"Tipo: <b>{periodo.capitalize()}</b>  ·  "
        f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}",
        s_normal))
    story.append(HRFlowable(width="100%", thickness=2, color=AZUL_CFE, spaceAfter=10))

    if resumen.empty or resumen.iloc[0]["lecturas"] == 0:
        story.append(Paragraph("⚠ Sin datos para el periodo seleccionado.", s_normal))
        doc.build(story)
        return buf.getvalue()

    r = resumen.iloc[0]
    story.append(Paragraph("Resumen del Periodo", s_seccion))

    kpis_data = [
        ["Lecturas", "Consumo Total", "Costo Total", "Picos", "Hogares"],
        [
            str(int(r["lecturas"])),
            fmt_kwh(r["kwh_total"]) + " kWh",
            fmt_mxn(r["costo_total"]) + " MXN",
            str(int(r["total_picos"] or 0)),
            str(int(r["hogares"])),
        ]
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
            filas_r, [1.8*inch, 1.4*inch, 1.4*inch, 0.8*inch, 0.9*inch], col_colors=[(2, ROJO_PDF)]
        ))
        story.append(Spacer(1, 6))

    if not por_hogar.empty:
        story.append(Paragraph("Ranking de Hogares por Consumo", s_seccion))
        filas_h = [[str(i+1), row["id_hogar"], row["ciudad"], row["region"],
                    fmt_kwh(row["kwh"]), fmt_mxn(row["costo"]), str(int(row["picos"] or 0))]
                   for i, (_, row) in enumerate(por_hogar.iterrows())]
        story.append(tabla(
            ["#", "Medidor", "Ciudad", "Región/Tarifa", "kWh", "Costo MXN", "Picos"],
            filas_h, [0.3*inch, 0.9*inch, 1.3*inch, 1.1*inch, 1.1*inch, 1.1*inch, 0.6*inch]
        ))
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
            filas_pk, [1.8*inch, 0.9*inch, 1.2*inch, 1.2*inch, 1.2*inch], col_colors=[(3, ROJO_PDF)]
        ))

    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.5, color=GRIS, spaceAfter=4))
    story.append(Paragraph(
        f"EnergyTrack · Proyecto Final UV 2026 · "
        f"Datos: Data Warehouse PostgreSQL · "
        f"Generado el {datetime.now().strftime('%d/%m/%Y a las %H:%M:%S')}", s_footer))

    doc.build(story)
    return buf.getvalue()

# ══════════════════════════════════════════════════════════
#  INICIALIZACIÓN DE LA APP DASH
# ══════════════════════════════════════════════════════════

app = Dash(__name__, external_stylesheets=[dbc.themes.CYBORG],
           title="EnergyTrack", suppress_callback_exceptions=True)

TAB  = {"background":CARD,"color":SUBTXT,"border":f"1px solid {BORDE}",
         "borderRadius":"8px 8px 0 0","padding":"10px 22px","fontWeight":"600"}
TABA = {**TAB,"color":ACCENT,"borderBottom":f"2px solid {ACCENT}","background":BG}

# Sidebar
sidebar = html.Div(style={
    "width":"220px","minWidth":"220px","background":CARD,
    "borderRight":f"1px solid {BORDE}","padding":"20px 14px",
    "display":"flex","flexDirection":"column","gap":"16px",
}, children=[
    html.Div([
        html.Div("⚡", style={"fontSize":"28px"}),
        html.H5("EnergyTrack", style={"color":ACCENT,"fontWeight":"800","margin":"0"}),
        html.P("Monitor energético · MX", style={"color":SUBTXT,"fontSize":"10px","margin":"0"}),
    ]),
    html.Hr(style={"borderColor":BORDE}),

    html.P("ESTADO DEL SISTEMA", style={"color":SUBTXT,"fontSize":"10px",
                                         "letterSpacing":"1.5px","margin":"0"}),
    html.Div(id="sb-estado"),

    html.Hr(style={"borderColor":BORDE}),
    html.P("ARQUITECTURA", style={"color":SUBTXT,"fontSize":"10px","letterSpacing":"1.5px","margin":"0"}),
    html.Div(id="sb-arq"),

    html.Hr(style={"borderColor":BORDE}),
    html.P("Proyecto Final · UV 2026",
           style={"color":SUBTXT,"fontSize":"10px","textAlign":"center","marginTop":"auto"}),
])

app.layout = html.Div(style={
    "display":"flex","background":BG,"minHeight":"100vh","fontFamily":"Inter, sans-serif"
}, children=[
    sidebar,
    html.Div(style={"flex":"1","padding":"24px","overflowY":"auto"}, children=[
        html.Div(style={"marginBottom":"20px"}, children=[
            html.H3("Dashboard de Consumo Energético",
                    style={"color":TEXTO,"margin":"0","fontWeight":"700"}),
            html.P("Fuente: Data Warehouse · Actualiza cada 10 segundos",
                   style={"color":SUBTXT,"margin":"4px 0 0","fontSize":"13px"}),
        ]),
        dcc.Tabs(id="tabs", value="metricas", children=[
            dcc.Tab(label="📊 Métricas",      value="metricas", style=TAB, selected_style=TABA),
            dcc.Tab(label="🔥 Consumo",       value="consumo",  style=TAB, selected_style=TABA),
            dcc.Tab(label="⚠️  Anomalías",    value="anomalias",style=TAB, selected_style=TABA),
            dcc.Tab(label="📄  Reportes PDF", value="reportes", style=TAB, selected_style=TABA),
        ], style={"marginBottom":"20px"}),
        html.Div(id="contenido"),
        dcc.Interval(id="tick", interval=10_000, n_intervals=0),
    ]),
])

# ══════════════════════════════════════════════════════════
#  CALLBACK SIDEBAR
# ══════════════════════════════════════════════════════════

@app.callback(
    Output("sb-estado","children"),
    Output("sb-arq","children"),
    Input("tick","n_intervals"),
)
def actualizar_sidebar(n):
    e = estado_sistema()

    if not e.get("dwh_ok"):
        estado = html.P("DWH no disponible.\nEjecuta pipeline.py",
                        style={"color":ROJO,"fontSize":"12px"})
    else:
        estado = html.Div([
            _sb_row("Lecturas", f"{e['total_lecturas']:,}", ACCENT),
            _sb_row("kWh total", fmt_kwh(e['kwh_total']), VERDE),
            _sb_row("Costo", fmt_mxn(e['costo_total']), AMBAR),
            _sb_row("Picos", f"{e['total_picos']:,}", ROJO),
            _sb_row("Hogares", f"{e['hogares_activos']}", SUBTXT),
        ])

    dwh_ok    = e.get("dwh_ok", False)
    lake_raw  = LAKE_RAW.exists() and bool(list(LAKE_RAW.rglob("*.parquet")))
    lake_proc = LAKE_PROCESSED.exists() and bool(list(LAKE_PROCESSED.rglob("*.parquet")))

    arq = html.Div([
        html.Div("● Kafka Local (Docker)", style={"color":VERDE if dwh_ok else SUBTXT,"fontSize":"11px","marginBottom":"5px"}),
        _sb_capa("ETL → Processed Respaldo",  lake_proc),
        _sb_capa("Pipeline Streaming → DWH", dwh_ok),
        _sb_capa("Dashboard Analítico ← DWH", dwh_ok),
    ])

    return estado, arq

def _sb_row(label, valor, color):
    return html.Div(style={"display":"flex","justifyContent":"space-between",
                            "marginBottom":"6px"}, children=[
        html.Span(label, style={"color":SUBTXT,"fontSize":"12px"}),
        html.Span(valor, style={"color":color,"fontSize":"12px","fontWeight":"700"}),
    ])

def _sb_capa(label, ok):
    dot = ("● " if ok else "○ ")
    color = VERDE if ok else SUBTXT
    return html.Div(dot + label, style={"color":color,"fontSize":"11px","marginBottom":"5px"})

# ══════════════════════════════════════════════════════════
#  ROUTING DE PESTAÑAS
# ══════════════════════════════════════════════════════════

@app.callback(Output("contenido","children"), Input("tabs","value"))
def render(tab):
    if tab == "metricas":  return tab_metricas()
    if tab == "consumo":   return tab_consumo()
    if tab == "anomalias": return tab_anomalias()
    return tab_reportes()

# PESTAÑA 1 — MÉTRICAS
def tab_metricas():
    return html.Div(id="m-root", children=_render_metricas())

def _render_metricas():
    e = estado_sistema()
    if not e.get("dwh_ok"):
        return _sin_datos()

    kpis = dbc.Row([
        kpi("Total lecturas",   f"{e['total_lecturas']:,}", "lecturas"),
        kpi("Consumo total",    fmt_kwh(e['kwh_total']),    "kWh",      VERDE),
        kpi("Costo estimado",   fmt_mxn(e['costo_total']),  "MXN",      AMBAR),
        kpi("Picos detectados", f"{e['total_picos']:,}",    "eventos",  ROJO),
    ], className="g-3 mb-4")

    df_r = dwh("SELECT region, SUM(kwh_total) AS kwh, SUM(costo_total) AS costo FROM v_consumo_por_region GROUP BY region ORDER BY kwh DESC")
    g_reg = go.Figure(go.Bar(
        x=df_r["region"], y=df_r["kwh"],
        marker_color=[PALETA.get(r,ACCENT) for r in df_r["region"]],
        text=df_r["kwh"].map(fmt_bar), textposition="outside",
    ))
    fig(g_reg, h=260)

    df_hog = dwh("SELECT id_hogar, ciudad, kwh_total AS kwh FROM v_consumo_por_hogar ORDER BY kwh ASC")
    colores = px.colors.sequential.Blues[2:]
    n = len(df_hog)
    cs = [colores[int(i/max(n-1,1)*(len(colores)-1))] for i in range(n)]
    g_rank = go.Figure(go.Bar(
        x=df_hog["kwh"], y=df_hog["id_hogar"]+" · "+df_hog["ciudad"],
        orientation="h", marker_color=cs,
        text=df_hog["kwh"].map(fmt_bar), textposition="outside",
    ))
    fig(g_rank, h=340)

    return html.Div([
        kpis,
        dbc.Row([
            dbc.Col(card("Consumo total por región  ← v_consumo_por_region",
                         dcc.Graph(figure=g_reg, config={"displayModeBar":False})), md=5),
            dbc.Col(card("Ranking de hogares  ← v_consumo_por_hogar",
                         dcc.Graph(figure=g_rank, config={"displayModeBar":False})), md=7),
        ], className="g-3"),
    ])

# PESTAÑA 2 — CONSUMO
def tab_consumo():
    if not estado_sistema().get("dwh_ok"):
        return _sin_datos()

    df_t = dwh("""
        SELECT fecha, region, ROUND(SUM(kwh_total)::numeric, 3) AS kwh
        FROM v_consumo_por_region GROUP BY fecha, region ORDER BY fecha
    """)
    g1 = go.Figure()
    
    if not df_t.empty and "fecha" in df_t.columns:
        df_t["fecha"] = pd.to_datetime(df_t["fecha"])
        for r in df_t["region"].unique():
            d = df_t[df_t["region"]==r]
            g1.add_trace(go.Scatter(x=d["fecha"],y=d["kwh"],name=r,mode="lines+markers",
                line=dict(color=PALETA.get(r,ACCENT),width=2),marker=dict(size=5)))
    else:
        g1.add_annotation(text="Sin datos de consumo",xref="paper",yref="paper",x=0.5,y=0.5,
                          showarrow=False,font=dict(color=SUBTXT,size=14))
        
    fig(g1)
    g1.update_xaxes(tickformat="%d %b", dtick="D1",
                    tickangle=-30, tickfont=dict(size=11))
    g1.update_yaxes(title_text="kWh")

    df_h = dwh("SELECT hora, ROUND(kwh_promedio::numeric,5) AS prom, ROUND(kwh_max::numeric,4) AS maximo FROM v_perfil_horario ORDER BY hora")
    g2 = go.Figure()
    
    if not df_h.empty and "hora" in df_h.columns:
        g2.add_trace(go.Scatter(x=df_h["hora"],y=df_h["prom"],name="Promedio",
            mode="lines+markers",line=dict(color=ACCENT,width=2.5),marker=dict(size=6)))
        g2.add_trace(go.Scatter(x=df_h["hora"],y=df_h["maximo"],name="Máximo",
            mode="lines",line=dict(color=ROJO,width=1.5,dash="dot")))
    
    fig(g2)
    g2.update_xaxes(tickmode="linear", tick0=0, dtick=1,
                    ticktext=[f"{h:02d}h" for h in range(24)],
                    tickvals=list(range(24)), tickangle=-30, tickfont=dict(size=10))
    g2.update_yaxes(title_text="kWh")

    df_all = dwh("""
        SELECT t.dia_semana, t.hora, AVG(f.kwh_intervalo) AS kwh
        FROM fact_consumo f JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        GROUP BY t.dia_semana, t.hora
    """)
    orden = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    trad  = {"Monday":"Lun","Tuesday":"Mar","Wednesday":"Mié",
             "Thursday":"Jue","Friday":"Vie","Saturday":"Sáb","Sunday":"Dom"}
    if not df_all.empty and "dia_semana" in df_all.columns:
        pivot = (df_all.pivot(index="dia_semana",columns="hora",values="kwh")
                       .reindex([d for d in orden if d in df_all["dia_semana"].values]))
        pivot.index = [trad.get(d,d) for d in pivot.index]
        g3 = go.Figure(go.Heatmap(
            z=pivot.values, x=[f"{h:02d}h" for h in pivot.columns], y=pivot.index,
            colorscale="Plasma", colorbar=dict(title="kWh",tickfont=dict(color=SUBTXT)),
        ))
    else:
        g3 = go.Figure()
    fig(g3, h=230)

    return html.Div([
        dbc.Row([
            dbc.Col(card("Consumo diario por región  ← v_consumo_por_region",
                         dcc.Graph(figure=g1,config={"displayModeBar":False})), md=7),
            dbc.Col(card("Curva de carga  ← v_perfil_horario",
                         dcc.Graph(figure=g2,config={"displayModeBar":False})), md=5),
        ], className="mb-3 g-3"),
        dbc.Row([
            dbc.Col(card("Mapa de calor — hora × día  ← fact_consumo + dim_tiempo",
                         dcc.Graph(figure=g3,config={"displayModeBar":False}), borde=AMBAR), md=12),
        ], className="g-3"),
    ])

# PESTAÑA 3 — ANOMALÍAS
def tab_anomalias():
    if not estado_sistema().get("dwh_ok"):
        return _sin_datos()

    df_p = dwh("""
        SELECT f.timestamp_real AS timestamp, r.region, f.kwh_intervalo
        FROM fact_consumo f
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE f.es_pico = true
        LIMIT 500
    """)
    g1 = go.Figure()
    
    if not df_p.empty and "timestamp" in df_p.columns:
        df_p["timestamp"] = pd.to_datetime(df_p["timestamp"])
        for r in df_p["region"].unique():
            d = df_p[df_p["region"]==r]
            g1.add_trace(go.Scatter(x=d["timestamp"],y=d["kwh_intervalo"],mode="markers",name=r,
                marker=dict(color=PALETA.get(r,ACCENT),size=8,opacity=0.85)))
    else:
        g1.add_annotation(text="Sin picos aún",xref="paper",yref="paper",x=0.5,y=0.5,
                          showarrow=False,font=dict(color=SUBTXT,size=14))
    fig(g1)
    g1.update_xaxes(tickformat="%d %b\n%H:%M", tickangle=0, tickfont=dict(size=10))
    g1.update_yaxes(title_text="kWh")

    df_ph = dwh("SELECT hora, num_picos FROM v_perfil_horario ORDER BY hora")
    todas_horas = pd.DataFrame({"hora": range(24)})
    
    if not df_ph.empty and "hora" in df_ph.columns:
        df_ph = todas_horas.merge(df_ph, on="hora", how="left").fillna(0)
    else:
        df_ph = todas_horas.copy()
        df_ph["num_picos"] = 0
        
    df_ph["etiqueta"] = df_ph["hora"].apply(lambda h: f"{int(h):02d}h")

    g2 = go.Figure(go.Bar(
        x=df_ph["etiqueta"], y=df_ph["num_picos"],
        marker_color=ROJO, opacity=0.8,
        text=df_ph["num_picos"].apply(lambda v: str(int(v)) if v > 0 else ""),
        textposition="outside",
    ))
    fig(g2, h=250)
    g2.update_xaxes(type="category", tickangle=-45, tickfont=dict(size=9))
    g2.update_yaxes(title_text="Número de picos")

    df_top = dwh("""
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
    
    tabla_obj = html.Div(style={"overflowX":"auto"}, children=[
        html.Table(style={"width":"100%","borderCollapse":"collapse","fontSize":"12px"},
            children=[
                html.結婚thead(html.Tr([
                    html.Th(col, style={"color":SUBTXT,"padding":"8px","borderBottom":f"1px solid {BORDE}",
                                        "textAlign":"left","textTransform":"uppercase","fontSize":"10px"})
                    for col in df_top.columns
                ])) if not df_top.empty else None,
                html.Tbody([
                    html.Tr([
                        html.Td(str(v), style={"color":TEXTO,"padding":"8px",
                                               "borderBottom":f"1px solid {BORDE}33"})
                        for v in row
                    ]) for row in df_top.values
                ]),
            ]
        )
    ]) if not df_top.empty else html.P("Sin datos de picos.", style={"color":SUBTXT})

    return html.Div([
        dbc.Row([
            dbc.Col(card("Dispersión temporal de picos",
                         dcc.Graph(figure=g1,config={"displayModeBar":False}),borde=ROJO), md=7),
            dbc.Col(card("Picos por hora  ← v_perfil_horario",
                         dcc.Graph(figure=g2,config={"displayModeBar":False}),borde=AMBAR), md=5),
        ], className="mb-3 g-3"),
        dbc.Row([
            dbc.Col(card("Top 10 picos", tabla_obj, borde=ROJO), md=12),
        ], className="g-3"),
    ])

# PESTAÑA 4 — REPORTES PDF
def tab_reportes():
    return html.Div([
        dcc.Download(id="descarga-pdf"),

        html.Div(style={
            "background": "linear-gradient(135deg, #1E3A5F 0%, #1E293B 100%)",
            "borderRadius": "12px", "padding": "24px", "marginBottom": "20px",
            "border": f"1px solid {BORDE}",
        }, children=[
            html.H4("📄 Generador de Reportes PDF", style={"color": TEXTO, "margin": "0 0 6px", "fontWeight": "700"}),
            html.P("Consulta el Data Warehouse y exporta el reporte del periodo que necesites.",
                   style={"color": SUBTXT, "margin": "0", "fontSize": "13px"}),
        ]),

        dbc.Row([
            dbc.Col(html.Div(style={
                "background": CARD, "borderRadius": "12px", "padding": "24px",
                "border": f"1px solid {BORDE}",
            }, children=[
                html.P("TIPO DE PERIODO", style={"color": SUBTXT, "fontSize": "10px",
                                                   "letterSpacing": "1.5px", "margin": "0 0 10px", "fontWeight": "700"}),
                dcc.RadioItems(
                    id="rep-tipo",
                    options=[
                        {"label": html.Span([html.Strong("Hora "), html.Span("— detalle por hora", style={"color": SUBTXT, "fontSize": "12px"})]), "value": "hora"},
                        {"label": html.Span([html.Strong("Día  "), html.Span("— resumen diario",    style={"color": SUBTXT, "fontSize": "12px"})]), "value": "dia"},
                        {"label": html.Span([html.Strong("Semana"), html.Span("— resumen semanal",  style={"color": SUBTXT, "fontSize": "12px"})]), "value": "semana"},
                        {"label": html.Span([html.Strong("Mes  "), html.Span("— resumen mensual",   style={"color": SUBTXT, "fontSize": "12px"})]), "value": "mes"},
                    ],
                    value="dia",
                    inputStyle={"marginRight": "8px", "accentColor": ACCENT},
                    labelStyle={"display": "block", "color": TEXTO, "fontSize": "14px",
                                "padding": "10px 12px", "borderRadius": "8px",
                                "marginBottom": "6px", "cursor": "pointer"},
                    style={"marginBottom": "20px"},
                ),

                html.P("PERIODO", style={"color": SUBTXT, "fontSize": "10px",
                                          "letterSpacing": "1.5px", "margin": "0 0 10px", "fontWeight": "700"}),
                html.Div(id="rep-selector-container"),
                html.Div(id="rep-valor-hidden", style={"display": "none"}),

                html.Div(style={"marginTop": "24px"}, children=[
                    html.Button(
                        "⬇  Generar y Descargar PDF",
                        id="rep-btn",
                        n_clicks=0,
                        style={
                            "width": "100%", "padding": "14px",
                            "background": f"linear-gradient(135deg, {ACCENT}, #0EA5E9)",
                            "color": "#0F172A", "border": "none", "borderRadius": "10px",
                            "fontWeight": "800", "fontSize": "14px", "cursor": "pointer",
                            "letterSpacing": "0.5px",
                        },
                    ),
                    html.Div(id="rep-status", style={"marginTop": "10px", "fontSize": "12px",
                                                      "color": SUBTXT, "textAlign": "center"}),
                ]),
            ]), md=4),

            dbc.Col(html.Div(style={
                "background": CARD, "borderRadius": "12px", "padding": "24px",
                "border": f"1px solid {BORDE}",
            }, children=[
                html.P("PREVISUALIZACIÓN", style={"color": SUBTXT, "fontSize": "10px",
                                                   "letterSpacing": "1.5px", "margin": "0 0 16px", "fontWeight": "700"}),
                html.Div(id="rep-preview"),
            ]), md=8),
        ], className="g-3"),
    ])

def _sin_datos():
    return html.Div(style={"textAlign":"center","padding":"60px"}, children=[
        html.Div("🏗", style={"fontSize":"48px"}),
        html.H4("Data Warehouse vacío", style={"color":TEXTO}),
        html.P("Ejecuta el pipeline para poblar el DWH:",
               style={"color":SUBTXT}),
        html.Code("python pipeline.py",
                  style={"color":ACCENT,"background":CARD,"padding":"8px 16px",
                         "borderRadius":"6px","fontSize":"14px"}),
        html.P(f"PostgreSQL: {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']}",
               style={"color":SUBTXT,"fontSize":"12px","marginTop":"8px"}),
    ])

# ══════════════════════════════════════════════════════════
#  CALLBACKS — REPORTES PDF (CON LA REPARACIÓN CRÍTICA)
# ══════════════════════════════════════════════════════════

@app.callback(
    Output("rep-selector-container", "children"),
    Input("rep-tipo", "value"),
)
def actualizar_selector(tipo):
    try:
        if tipo == "hora":
            opts = dwh("SELECT DISTINCT fecha||' '||hora AS fh FROM dim_tiempo ORDER BY fh DESC LIMIT 72")["fh"].tolist()
            opts_fmt = []
            for fh in opts:
                if fh:
                    fecha_part, hora_part = str(fh).split(" ")
                    label = f"{hora_part.zfill(2)}:00 h — {fecha_part}"
                    opts_fmt.append({"label": label, "value": str(fh)})
                    
        elif tipo == "dia":
            opts = dwh("SELECT DISTINCT fecha FROM dim_tiempo ORDER BY fecha DESC LIMIT 90")["fecha"].tolist()
            # 💡 SOLUCIÓN: Casteamos explícitamente a string cada fecha nativa de Postgres
            opts_fmt = [{"label": str(o), "value": str(o)} for o in opts if o]
            
        elif tipo == "semana":
            opts = dwh("SELECT DISTINCT year||'-W'||LPAD(EXTRACT(WEEK FROM fecha::date)::text,2,'0') AS sem FROM dim_tiempo ORDER BY sem DESC")["sem"].tolist()
            opts_fmt = [{"label": str(o), "value": str(o)} for o in opts if o]
            
        else:  # mes
            opts = dwh("SELECT DISTINCT year||'-'||LPAD(month::text,2,'0') AS mes FROM dim_tiempo ORDER BY mes DESC")["mes"].tolist()
            opts_fmt = [{"label": str(o), "value": str(o)} for o in opts if o]
            
    except Exception as e:
        print(f"❌ Error en actualizar_selector: {e}")
        opts_fmt = []

    if not opts_fmt:
        return html.P("Sin datos en el DWH aún.", style={"color": SUBTXT, "fontSize": "12px"})

    return dbc.Select(
        id="rep-valor", 
        options=opts_fmt, 
        value=opts_fmt[0]["value"] if opts_fmt else None,
        style={
            "backgroundColor": BG, 
            "color": TEXTO, 
            "borderColor": BORDE,
            "cursor": "pointer"
        },
    )


@app.callback(
    Output("rep-preview", "children"),
    Input("rep-tipo", "value"),
    Input("rep-valor", "value"),
    prevent_initial_call=True,
)
def preview_reporte(tipo, valor):
    if not valor:
        return html.P("Selecciona un periodo.", style={"color": SUBTXT})

    where = _where_periodo(tipo, valor)
    label = _label_periodo(tipo, valor)

    r = dwh(f"""
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
        return html.Div([
            html.Div("⚠", style={"fontSize":"32px","textAlign":"center","marginBottom":"8px"}),
            html.P(f"Sin datos para: {label}", style={"color":SUBTXT,"textAlign":"center"}),
        ])

    row = r.iloc[0]
    def stat(titulo, valor, color=TEXTO):
        return html.Div(style={
            "background": BG, "borderRadius": "8px", "padding": "12px 16px",
            "border": f"1px solid {BORDE}", "marginBottom": "8px",
            "display": "flex", "justifyContent": "space-between",
        }, children=[
            html.Span(titulo, style={"color": SUBTXT, "fontSize": "12px"}),
            html.Span(str(valor), style={"color": color, "fontWeight": "700", "fontSize": "13px"}),
        ])

    reg = dwh(f"""
        SELECT r.region, ROUND(SUM(f.kwh_intervalo)::numeric,6) AS kwh
        FROM fact_consumo f
        JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
        JOIN dim_region r ON f.region_key = r.region_key
        WHERE {where}
        GROUP BY r.region ORDER BY kwh DESC
    """)

    reg_items = []
    for _, rrow in reg.iterrows():
        reg_items.append(html.Div(style={
            "display":"flex","justifyContent":"space-between",
            "padding":"4px 0","borderBottom":f"1px solid {BORDE}33",
        }, children=[
            html.Span(rrow["region"], style={"color":TEXTO,"fontSize":"12px"}),
            html.Span(fmt_kwh(rrow["kwh"])+" kWh", style={"color":ACCENT,"fontSize":"12px","fontWeight":"600"}),
        ]))

    return html.Div([
        html.P(f"📋 {label}", style={"color":ACCENT,"fontWeight":"700","fontSize":"14px","marginBottom":"14px"}),
        stat("Total lecturas",  f"{int(row['lecturas']):,}",         ACCENT),
        stat("Consumo total",   fmt_kwh(row["kwh"]) + " kWh",       VERDE),
        stat("Costo estimado",  fmt_mxn(row["costo"]) + " MXN",     AMBAR),
        stat("Picos detectados",f"{int(row['picos'] or 0)}",         ROJO),
        stat("Hogares activos", f"{int(row['hogares'])}",            SUBTXT),
        html.P("Distribución por región:", style={"color":SUBTXT,"fontSize":"11px",
                                                   "marginTop":"16px","marginBottom":"6px",
                                                   "textTransform":"uppercase","letterSpacing":"1px"}),
        html.Div(reg_items),
        html.Div(style={"marginTop":"16px","padding":"10px","background":BG,
                        "borderRadius":"8px","border":f"1px solid {VERDE}33"}, children=[
            html.Span("✓ ", style={"color":VERDE}),
            html.Span("El PDF incluirá: resumen, consumo por región, "
                      "ranking de hogares, perfil horario y tabla de anomalías.",
                      style={"color":SUBTXT,"fontSize":"11px"}),
        ]),
    ])


@app.callback(
    Output("descarga-pdf", "data"),
    Output("rep-status", "children"),
    Input("rep-btn", "n_clicks"),
    Input("rep-tipo", "value"),
    Input("rep-valor", "value"),
    prevent_initial_call=True,
)
def descargar_pdf(n_clicks, tipo, valor):
    from dash import ctx
    if ctx.triggered_id != "rep-btn" or not n_clicks or not valor:
        return None, ""

    try:
        pdf_bytes = generar_pdf(tipo, valor)
        label_clean = valor.replace(" ", "_").replace(":", "-").replace("/", "-")
        nombre = f"EnergyTrack_{tipo}_{label_clean}.pdf"
        pdf_b64 = base64.b64encode(pdf_bytes).decode()
        return (
            {"base64": True, "content": pdf_b64, "filename": nombre, "type": "application/pdf"},
             f"✓ Reporte generado: {nombre}"
        )
    except Exception as e:
        return None, f"✗ Error: {str(e)}"

# ══════════════════════════════════════════════════════════
#  PUNTO DE ARRANQUE DEL DASHBOARD
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n  ╔══════════════════════════════════════════════╗")
    print("  ║  EnergyTrack — Dashboard (PRODUCTIVO)        ║")
    print("  ║  Fuente: Data Warehouse únicamente           ║")
    print("  ║                                              ║")
    print("  ║  Terminal 1: python simulador_streaming.py   ║")
    print("  ║  Terminal 2: python pipeline.py              ║")
    print("  ║  Terminal 3: python dashboard.py             ║")
    print("  ║  Navegador:  http://127.0.0.1:8050           ║")
    print("  ╚══════════════════════════════════════════════╝\n")
    app.run(debug=False, host="127.0.0.1", port=8050)