import os, io, re, json, time, threading, logging
from datetime import datetime
import pandas as pd
import dropbox
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# ── Configuración ──────────────────────────────────────────────
DROPBOX_TOKEN        = os.environ.get('DROPBOX_TOKEN', '')
DROPBOX_PRESUPUESTO  = os.environ.get('DROPBOX_PRESUPUESTO',  '/Presupuesto.xlsx')
DROPBOX_BASICA       = os.environ.get('DROPBOX_BASICA',       '/Nueva/BASICA 2026.xlsx')
REFRESH_MINUTES      = int(os.environ.get('REFRESH_MINUTES', '120'))

# Cache en memoria
_cache = {'data': None, 'last_update': None, 'error': None}

# ── Constantes de mapeo ────────────────────────────────────────
MES_MAP = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
           'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}

UN_TO_CC = {
    'ADMINISTRACION':'ADMINISTRATIVO','ADMINISTRACIÓN':'ADMINISTRATIVO','ADMON':'ADMINISTRATIVO',
    'CHILCO':'CHILCO','COPSERVIR':'COOPSERVIR','MULAS PROPIAS':'MULAS PROPIAS',
    'OXIGENOS':'PRAXAIR','OXÍGENOS':'PRAXAIR',
}

RUBROS     = ['venta_real','nomina','combustible','mtto','gastos_admi','bo','polizas','soat','rtmc','impuestos','capital']
GASTO_RUBROS = [r for r in RUBROS if r != 'venta_real']

def is_placa(val):
    if pd.isna(val): return False
    return bool(re.match(r'^[A-Z]{2,3}\d{3,4}$', str(val).strip().upper()))

def map_rubro(n1, n2, n3, n4):
    n1 = str(n1).upper().strip() if n1 else ''
    n2 = str(n2).upper().strip() if n2 else ''
    n3 = str(n3).upper().strip() if n3 else ''
    n4 = str(n4).upper().strip() if n4 else ''
    if 'INGRESO' in n1 and 'OPERACIONAL' in n2 and 'DEVOLUCION' not in n3:
        return 'venta_real'
    if ('OPERACIONALES DE VENTAS' in n2 or 'OPERACIONALES DE ADMINISTRACION' in n2) and 'GASTOS DE PERSONAL' in n3:
        return 'nomina'
    if 'COMBUSTIBLE' in n4 or 'LUBRICANTE' in n4:
        return 'combustible'
    if 'MANTENIMIENTO' in n3 and ('FLOTA' in n4 or 'MAQUINARIA' in n4 or 'EQUIPO' in n4):
        return 'mtto'
    if 'OBLIGATORIO ACCIDENTE' in n4 or 'SOAT' in n4:
        return 'soat'
    if ('SEGUROS' in n3 and 'OBLIGATORIO' not in n4) or 'POLIZAS' in n4:
        return 'polizas'
    if 'TECNICOMECANICA' in n4 or 'REVISION TECNICO' in n4:
        return 'rtmc'
    if 'DE VEHICULOS' in n4 and 'IMPUESTO' in n3:
        return 'impuestos'
    if 'GASTOS DE VIAJE' in n3:
        return 'gastos_admi'
    if 'SERVICIOS' in n3 and ('TRANSPORTE' in n4 or 'FLETE' in n4):
        return 'bo'
    if 'DEPRECIACION' in n3 and 'FLOTA' in n4:
        return 'capital'
    return None

def clean(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return ''
    return str(v).strip()

# ── Procesamiento ──────────────────────────────────────────────
def process_excel(presupuesto_bytes, basica_bytes):
    log.info("Procesando archivos...")

    # ── Hoja Presupuesto (Presupuesto.xlsx → hoja "Presupuesto") ──
    df_p = pd.read_excel(io.BytesIO(presupuesto_bytes), sheet_name='Presupuesto', header=None)
    mes_start = {1:2,2:18,3:34,4:50,5:66,6:82,7:98,8:114,9:130,10:146,11:162,12:178}

    presup_records = []
    for _, row in df_p.iloc[2:].iterrows():
        cc    = str(row[0]).strip() if pd.notna(row[0]) else ''
        placa = str(row[1]).strip() if pd.notna(row[1]) else ''
        if not cc or cc in ['nan',''] or not placa or placa in ['nan','']: continue
        for mes, start in mes_start.items():
            def v(offset):
                val = row[start+offset]
                return 0 if pd.isna(val) else float(val) if str(val).replace('.','').replace('-','').isdigit() else 0
            presup_records.append({
                'placa':placa,'cc':cc,'modelo':str(row[start]).strip(),'mes':mes,
                'venta':v(1),'gastos_admi':v(2),'bo':v(3),'mtto':v(4),
                'combustible':v(5),'sst':v(6),'nomina':v(7),'polizas':v(8),
                'impuestos':v(9),'soat':v(10),'rtmc':v(11),'avaluo':v(12),
                'capital':v(13),'total':v(14),
            })
    del df_p
    log.info(f"Presupuesto: {len(presup_records)} registros")

    # ── Hoja BASICA — solo columnas necesarias para ahorrar RAM ──
    COLS_NEEDED = [
        'Mes', 'Centro de Costo', 'Nombre Centro de Costo',
        'nombre Unidad de Negocio', 'Nombre_cuenta_n1', 'Nombre_cuenta_n2',
        'Nombre_cuenta_n3', 'Nombre_cuenta_n4', 'Nombre_auxiliar',
        'Nombre Tercero', 'Notas', 'Movto_libro2'
    ]

    df_b = pd.read_excel(
        io.BytesIO(basica_bytes), sheet_name='BASICA', header=0,
        usecols=COLS_NEEDED, dtype={'Centro de Costo': str, 'Movto_libro2': float}
    )
    del basica_bytes  # liberar memoria del archivo crudo

    df_b['mes_num']   = df_b['Mes'].str.lower().str.strip().map(MES_MAP)
    df_b['placa']     = df_b['Centro de Costo'].apply(lambda x: str(x).strip().upper() if is_placa(x) else None)
    df_b['cc_presup'] = df_b['nombre Unidad de Negocio'].str.strip().str.upper().map(UN_TO_CC)
    df_b['rubro']     = df_b.apply(lambda r: map_rubro(
        r['Nombre_cuenta_n1'], r['Nombre_cuenta_n2'],
        r['Nombre_cuenta_n3'], r.get('Nombre_cuenta_n4','')), axis=1)

    real = df_b[
        df_b['placa'].notna() & df_b['rubro'].notna() &
        df_b['mes_num'].notna() & df_b['cc_presup'].notna() &
        (df_b['Movto_libro2'].fillna(0) != 0)
    ].copy()

    del df_b  # liberar dataframe completo
    log.info(f"BASICA filtrada: {len(real)} filas útiles")
    df_b['mes_num']  = df_b['Mes'].str.lower().str.strip().map(MES_MAP)
    df_b['placa']    = df_b['Centro de Costo'].apply(lambda x: str(x).strip().upper() if is_placa(x) else None)
    df_b['cc_presup']= df_b['nombre Unidad de Negocio'].str.strip().str.upper().map(UN_TO_CC)
    df_b['rubro']    = df_b.apply(lambda r: map_rubro(
        r['Nombre_cuenta_n1'], r['Nombre_cuenta_n2'],
        r['Nombre_cuenta_n3'], r.get('Nombre_cuenta_n4','')), axis=1)

    # Pivot real por placa+mes
    pivoted = {}
    for _, row in real.iterrows():
        key = (row['placa'], int(row['mes_num']))
        if key not in pivoted:
            pivoted[key] = {
                'placa':row['placa'],'mes':int(row['mes_num']),
                'unidad_negocio':clean(row['nombre Unidad de Negocio']),
                'cc':clean(row['cc_presup']),
                'cc_nombre':clean(row['Nombre Centro de Costo'])
            }
            for rb in RUBROS: pivoted[key][rb] = 0.0
        pivoted[key][row['rubro']] = pivoted[key].get(row['rubro'],0.0) + abs(float(row['Movto_libro2']))

    real_records = list(pivoted.values())
    for r in real_records:
        r['total_real'] = sum(r.get(rb,0) for rb in GASTO_RUBROS)

    # Árbol cascada N2→N3→Auxiliar→Tercero→Notas
    tree_raw = {}
    for _, row in real.iterrows():
        key = f"{row['placa']}|{int(row['mes_num'])}|{row['rubro']}"
        if key not in tree_raw: tree_raw[key] = {}
        n2   = clean(row['Nombre_cuenta_n2'])
        n3   = clean(row['Nombre_cuenta_n3'])
        aux  = clean(row['Nombre_auxiliar'])  or '(sin auxiliar)'
        ter  = clean(row['Nombre Tercero'])   or '(sin tercero)'
        nota = clean(row['Notas'])
        val  = abs(float(row['Movto_libro2']))
        t = tree_raw[key]
        if n2 not in t: t[n2] = {'_val':0,'_children':{}}
        t[n2]['_val'] += val
        c2 = t[n2]['_children']
        if n3 not in c2: c2[n3] = {'_val':0,'_children':{}}
        c2[n3]['_val'] += val
        c3 = c2[n3]['_children']
        if aux not in c3: c3[aux] = {'_val':0,'_children':{}}
        c3[aux]['_val'] += val
        c4 = c3[aux]['_children']
        if ter not in c4: c4[ter] = {'_val':0,'_notes':[]}
        c4[ter]['_val'] += val
        if nota: c4[ter]['_notes'].append({'n':nota[:80],'v':round(val)})

    def optimize(node, is_gadmi=False):
        r = {'v': round(node.get('_val',0))}
        ch = node.get('_children',{})
        if ch:
            items = sorted(ch.items(), key=lambda x: -x[1].get('_val',0))[:20]
            r['c'] = {k[:60]: optimize(v, is_gadmi) for k,v in items}
        notes = node.get('_notes',[])
        if notes and not is_gadmi:
            acc = {}
            for n in notes:
                acc[n['n']] = acc.get(n['n'],0)+n['v']
            r['n'] = [{'t':t,'v':v} for t,v in sorted(acc.items(),key=lambda x:-x[1])[:5] if t]
        return r

    tree = {}
    for key, node in tree_raw.items():
        rubro = key.split('|')[2]
        is_gadmi = rubro == 'gastos_admi'
        tree[key] = {n2[:60]: optimize(n2d, is_gadmi) for n2, n2d in node.items()}

    # Obtener meses reales disponibles
    meses_reales = sorted(set(r['mes'] for r in real_records))

    log.info(f"Procesado OK — {len(presup_records)} presup, {len(real_records)} real, {len(tree)} árbol")
    return {
        'presupuesto': presup_records,
        'real': real_records,
        'tree': tree,
        'meses_reales': meses_reales,
        'last_update': datetime.now().isoformat(),
    }

def download_and_process():
    global _cache
    try:
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)

        log.info(f"Descargando presupuesto: {DROPBOX_PRESUPUESTO}")
        _, res_p = dbx.files_download(DROPBOX_PRESUPUESTO)
        presupuesto_bytes = res_p.content
        log.info(f"Presupuesto descargado: {len(presupuesto_bytes)/1024:.0f} KB")

        log.info(f"Descargando contabilidad: {DROPBOX_BASICA}")
        _, res_b = dbx.files_download(DROPBOX_BASICA)
        basica_bytes = res_b.content
        log.info(f"BASICA descargada: {len(basica_bytes)/1024:.0f} KB")

        data = process_excel(presupuesto_bytes, basica_bytes)
        _cache['data']        = data
        _cache['last_update'] = datetime.now().isoformat()
        _cache['error']       = None
        log.info("Cache actualizado OK")
    except Exception as e:
        _cache['error'] = str(e)
        log.error(f"Error actualizando cache: {e}")

def scheduler():
    while True:
        download_and_process()
        log.info(f"Próxima actualización en {REFRESH_MINUTES} minutos")
        time.sleep(REFRESH_MINUTES * 60)

# ── Rutas API ─────────────────────────────────────────────────
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/data')
def get_data():
    if _cache['data'] is None:
        if _cache['error']:
            return jsonify({'error': _cache['error']}), 503
        return jsonify({'error': 'Datos cargando, intenta en 30 segundos'}), 503
    return jsonify({
        'presupuesto':  _cache['data']['presupuesto'],
        'real':         _cache['data']['real'],
        'tree':         _cache['data']['tree'],
        'meses_reales': _cache['data']['meses_reales'],
        'last_update':  _cache['data']['last_update'],
    })

@app.route('/api/status')
def status():
    return jsonify({
        'ok':           _cache['data'] is not None,
        'last_update':  _cache['last_update'],
        'error':        _cache['error'],
        'refresh_every': f'{REFRESH_MINUTES} minutos',
    })

@app.route('/api/refresh', methods=['POST'])
def force_refresh():
    threading.Thread(target=download_and_process, daemon=True).start()
    return jsonify({'message': 'Actualización iniciada'})

# ── Inicio ────────────────────────────────────────────────────
# Arrancar scheduler al importar (funciona con gunicorn)
_scheduler_thread = threading.Thread(target=scheduler, daemon=True)
_scheduler_thread.start()
log.info("Scheduler iniciado — primera descarga en progreso...")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
