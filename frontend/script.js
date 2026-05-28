// CONFIGURACIÓN BASE
const API = "http://127.0.0.1:5000";

const PALETA = {
  "1":  "#2563EB",
  "1B": "#059669",
  "1C": "#D97706",
  "1E": "#DC2626",
  "1F": "#7C3AED",
};

const layoutBase = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: '#F5F5F5',
  font: { color: '#374151', size: 12, family: 'Inter, system-ui, sans-serif' },
  margin: { l: 40, r: 20, t: 24, b: 30 },
  xaxis: { gridcolor: '#C0C7CF', linecolor: '#9CA3AF', zerolinecolor: '#C0C7CF' },
  yaxis: { gridcolor: '#C0C7CF', linecolor: '#9CA3AF', zerolinecolor: '#C0C7CF' },
  legend: { bgcolor: 'rgba(255,255,255,0.9)', orientation: 'h', y: 1.12, font: {color: '#4B5563', weight: 500} }
};

const configBase = { displayModeBar: false, responsive: true };

function buildLayout(extra) {
  return Object.assign({}, JSON.parse(JSON.stringify(layoutBase)), extra);
}

function fmt(v, t) {
  v = parseFloat(v) || 0;
  if (t === 'kwh') return v >= 1000 ? v.toLocaleString('en-US',{minimumFractionDigits:1}) : v >= 1 ? v.toFixed(3) : v.toFixed(4);
  return v >= 100 ? "$" + v.toLocaleString('en-US',{minimumFractionDigits:2}) : "$" + v.toFixed(3);
}

let currentTab = 'metricas';

function switchTab(tabId, element) {
  currentTab = tabId;
  document.querySelectorAll('.nav-item').forEach(item => item.classList.remove('active'));
  if (element) element.classList.add('active');
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  document.getElementById(tabId).classList.add('active');
  fetchData(); 
}

// PETICIONES A LA API
async function fetchData() {
  try {
    const res = await fetch(`${API}/api/resumen`).then(r => r.json());
    if (res && res.ok) {
      document.getElementById('sb-estado').innerHTML = `
        <div class="sb-row"><span class="lbl">Lecturas</span><span class="val">${res.total_lecturas.toLocaleString()}</span></div>
        <div class="sb-row"><span class="lbl">kWh total</span><span class="val">${fmt(res.kwh_total, 'kwh')}</span></div>
        <div class="sb-row"><span class="lbl">Costo</span><span class="val">${fmt(res.costo_total, 'mxn')}</span></div>
        <div class="sb-row"><span class="lbl">Picos</span><span class="val">${res.total_picos.toLocaleString()}</span></div>
        <div class="sb-row"><span class="lbl">Hogares</span><span class="val">${res.hogares_activos}</span></div>
      `;
      document.getElementById('kpi-lect').innerText = res.total_lecturas.toLocaleString();
      document.getElementById('kpi-kwh').innerText = fmt(res.kwh_total, 'kwh');
      document.getElementById('kpi-costo').innerText = fmt(res.costo_total, 'mxn');
      document.getElementById('kpi-picos').innerText = res.total_picos.toLocaleString();
    }

    if (currentTab === 'metricas') {
      const dReg = await fetch(`${API}/api/consumo-region`).then(r => r.json());
      if(dReg && dReg.length) {
        const xR = dReg.map(d => d.region);
        const yR = dReg.map(d => Number(d.kwh || 0));
        Plotly.react('plot-regiones', [{
          x: xR, y: yR, type: 'bar',
          marker: { color: xR.map(r => PALETA[r] || '#2563EB') },
          text: yR.map(v => v.toFixed(2)), textposition: 'outside'
        }], buildLayout({ margin: { l: 40, r: 20, t: 24, b: 20 } }), configBase);
      }

      const dHog = await fetch(`${API}/api/ranking-hogares`).then(r => r.json());
      if(dHog && dHog.length) {
        const xH = dHog.map(d => Number(d.kwh || 0));
        const yH = dHog.map(d => `${d.id_hogar} · ${d.ciudad}`);
        Plotly.react('plot-hogares', [{
          x: xH, y: yH, type: 'bar', orientation: 'h',
          marker: { color: '#2563EB', opacity: 0.9 },
          text: xH.map(v => v.toFixed(2)), textposition: 'outside'
        }], buildLayout({ margin: { l: 150, r: 40, t: 24, b: 20 } }), configBase);
      }
    }
    
    else if (currentTab === 'consumo') {
      const dSerie = await fetch(`${API}/api/serie-diaria`).then(r => r.json());
      if(dSerie && dSerie.length) {
        const regions = [...new Set(dSerie.map(d => d.region))];
        const trazosSerie = regions.map(r => {
          const dataR = dSerie.filter(d => d.region === r);
          return { x: dataR.map(d=>d.fecha), y: dataR.map(d=>Number(d.kwh||0)), name: r, mode: 'lines+markers', line: {color: PALETA[r]||'#2563EB', width: 2.2}, marker: {size: 5} };
        });
        const layoutS = buildLayout({ margin: {l:40, r:20, t:24, b:30} });
        layoutS.yaxis.title = 'kWh';
        Plotly.react('plot-serie', trazosSerie, layoutS, configBase);
      }

      const dCurva = await fetch(`${API}/api/perfil-horario`).then(r => r.json());
      if(dCurva && dCurva.length) {
        Plotly.react('plot-curva', [
          { x: dCurva.map(d=>d.hora), y: dCurva.map(d=>Number(d.prom||0)), name: 'Promedio', mode: 'lines+markers', line: {color: '#2563EB', width: 2.2} },
          { x: dCurva.map(d=>d.hora), y: dCurva.map(d=>Number(d.maximo||0)), name: 'Máximo', mode: 'lines', line: {color: '#DC2626', width: 1.8, dash: 'dot'} }
        ], buildLayout({ xaxis: { tickvals: [...Array(24).keys()], ticktext: [...Array(24).keys()].map(h=>`${String(h).padStart(2,'0')}h`), gridcolor:'#C0C7CF' } }), configBase);
      }

      const dHeat = await fetch(`${API}/api/heatmap`).then(r => r.json());
      if(dHeat && dHeat.length) {
        const tradDias = ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"];
        const diasPosibles = [
          ["monday", "lunes"], ["tuesday", "martes"], ["wednesday", "miércoles", "miercoles"],
          ["thursday", "jueves"], ["friday", "viernes"], ["saturday", "sábado", "sabado"], ["sunday", "domingo"]
        ];
        const z = [];
        for (let i = 0; i < 7; i++) {
          const row = [];
          for (let h = 0; h < 24; h++) {
            const item = dHeat.find(d => diasPosibles[i].includes((d.dia_semana || "").toLowerCase().trim()) && Number(d.hora) === h);
            row.push(item ? Number(item.kwh || 0) : 0);
          }
          z.push(row);
        }
        
        Plotly.react('plot-heatmap', [{
          z: z, x: [...Array(24).keys()].map(h=>`${String(h).padStart(2,'0')}h`), y: tradDias,
          type: 'heatmap',
          zmin: 0, 
          xgap: 2, ygap: 2,
          colorscale: [
            [0, '#F5F5F5'], [0.15, '#BFDBFE'], [0.4, '#3B82F6'], [0.6, '#059669'], [0.8, '#D97706'], [1.0, '#DC2626']
          ],
          colorbar: { tickfont: { color: '#525252' }, title: { text: 'kWh', font: {color: '#525252'} } }
        }], buildLayout({ margin: {l:40, r:10, t:10, b:30} }), configBase);
      }
    }

    else if (currentTab === 'anomalias') {
      const dDisp = await fetch(`${API}/api/picos`).then(r => r.json());
      if(dDisp && dDisp.length) {
        const regions = [...new Set(dDisp.map(d => d.region))];
        Plotly.react('plot-dispersion', regions.map(r => {
          const dataR = dDisp.filter(d => d.region === r);
          return { x: dataR.map(d=>d.timestamp), y: dataR.map(d=>Number(d.kwh_intervalo||0)), name: r, mode: 'markers', marker: {color: PALETA[r]||'#DC2626', size: 8} };
        }), buildLayout({ yaxis: {title: 'kWh'} }), configBase);
      }

      const dHora = await fetch(`${API}/api/picos-por-hora`).then(r => r.json());
      if(dHora && dHora.length) {
        const xH = dHora.map(d=>`${String(d.hora).padStart(2,'0')}h`);
        const yH = dHora.map(d=>Number(d.num_picos||0));
        Plotly.react('plot-phoraria', [{
          x: xH, y: yH, type: 'bar', marker: {color: '#DC2626', opacity: 0.9}, 
          text: yH.map(v=>v>0 ? v : ""), textposition: 'outside'
        }], buildLayout({ yaxis: {title: 'Número de picos'} }), configBase);
      }

      const dTop = await fetch(`${API}/api/top-picos`).then(r => r.json());
      if(dTop && dTop.length) {
        document.getElementById('tabla-body').innerHTML = dTop.map(r => `
          <tr><td>${r.timestamp.substring(0,19)}</td><td>${r.ciudad}</td><td>${r.region}</td>
          <td style="color:#DC2626;font-weight:600;">${Number(r.kwh||0).toFixed(4)}</td><td>$${Number(r.costo||0).toFixed(2)}</td></tr>
        `).join('');
      }
    }
  } catch (err) { console.error("Error obteniendo datos de la API", err); }
}

// REPORTES PDF
async function cargarSelect() {
  const tipo = document.querySelector('input[name="rtipo"]:checked').value;
  const opts = await fetch(`${API}/api/periodos?tipo=${tipo}`).then(r => r.json());
  const sel = document.getElementById("rvalor");
  sel.innerHTML = opts.length ? opts.map(o => `<option value="${o.value}">${o.label}</option>`).join('') : '<option value="">Sin datos</option>';
  if(opts.length) cargarPreview();
}

async function cargarPreview() {
  const tipo = document.querySelector('input[name="rtipo"]:checked').value;
  const valor = document.getElementById("rvalor").value;
  if (!valor) return;
  const d = await fetch(`${API}/api/preview?tipo=${tipo}&valor=${encodeURIComponent(valor)}`).then(r => r.json());
  const div = document.getElementById("rep-preview");
  if (!d || !d.ok) { div.innerHTML = `<p style="color:#DC2626;">Sin datos para este periodo.</p>`; return; }
  
  const regHtml = d.regiones.map(r => `<div class="reg-item"><span>${r.region}</span><span style="font-weight:600;">${fmt(r.kwh,'kwh')} kWh</span></div>`).join('');
  div.innerHTML = `
    <p style="font-weight:700; margin-bottom:18px; font-size:16px;">${d.label}</p>
    <div class="stat-item"><span style="color:var(--subtxt);">Total lecturas</span><span style="font-weight:600;">${d.lecturas.toLocaleString()}</span></div>
    <div class="stat-item"><span style="color:var(--subtxt);">Consumo total</span><span style="font-weight:600;">${d.kwh_fmt} kWh</span></div>
    <div class="stat-item"><span style="color:var(--subtxt);">Costo estimado</span><span style="font-weight:600;">${d.costo_fmt} MXN</span></div>
    <div class="stat-item"><span style="color:var(--subtxt);">Picos detectados</span><span style="font-weight:600;">${d.picos}</span></div>
    <p style="color:var(--subtxt); font-size:11px; margin:20px 0 8px; text-transform:uppercase; font-weight:600;">Distribución por región:</p>
    ${regHtml}
  `;
}

function descargarPDF() {
  const tipo = document.querySelector('input[name="rtipo"]:checked').value;
  const valor = document.getElementById("rvalor").value;
  if(!valor) return;
  window.open(`${API}/api/reporte-pdf?tipo=${tipo}&valor=${encodeURIComponent(valor)}`, '_blank');
}

document.querySelectorAll('input[name="rtipo"]').forEach(r => r.addEventListener('change', cargarSelect));
document.getElementById('rvalor').addEventListener('change', cargarPreview);

cargarSelect();
fetchData();
setInterval(fetchData, 10000);