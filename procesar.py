"""
CCM Fleet Budget — Procesador local
Corre este script cada vez que actualices los archivos Excel.
Genera el JSON procesado y lo sube a Dropbox automáticamente.

Instalación (solo la primera vez):
    pip install pandas openpyxl dropbox

Uso:
    python procesar.py
"""

import io, re, json, sys
from datetime import datetime
import pandas as pd
import dropbox

# ─── CONFIGURACIÓN ────────────────────────────────────────────
APP_KEY       = "i3qh1or39zreiih"
APP_SECRET    = "tzqnzdw1xvwwnwg"
REFRESH_TOKEN = "hQTVhFF7Oa4AAAAAAAAAAWPGkeIttW-BgqwmcC3QF_9vw7q8vcDk1h1SlqDiA1-5"

DROPBOX_PRESUPUESTO = "/Presupuesto.xlsx"
DROPBOX_BASICA      = "/Nueva/BASICA 2026.xlsx"
DROPBOX_VIAJES      = "/cloudfleet_viajes.xlsx"
DROPBOX_JSON_OUT    = "/ccm_data.json"

# ─── CONSTANTES ───────────────────────────────────────────────
MES_MAP = {'enero':1,'febrero':2,'marzo':3,'abril':4,'mayo':5,'junio':6,
           'julio':7,'agosto':8,'septiembre':9,'octubre':10,'noviembre':11,'diciembre':12}

UN_TO_CC = {
    'ADMINISTRACION':'ADMINISTRATIVO','ADMINISTRACIÓN':'ADMINISTRATIVO','ADMON':'ADMINISTRATIVO',
    'CHILCO':'CHILCO','COPSERVIR':'COOPSERVIR','MULAS PROPIAS':'MULAS PROPIAS',
    'OXIGENOS':'PRAXAIR','OXÍGENOS':'PRAXAIR','TRANSFERENCIAS OXIGENOS':'PRAXAIR',
    'CARGA':'CARGA','BIG COLA':'BIG COLA','INTERNACIONAL':'INTERNACIONAL',
    'SERVICIO DE TRANSPORTE OPERATIVO':'SERVICIO DE TRANSPORTE OPERATIVO',
    'LIQUIDOS':'LIQUIDOS',
}

RUBROS      = ['venta_real','nomina','combustible','mtto','gastos_admi','bo','polizas','soat','rtmc','impuestos','capital']
GASTO_RUBROS= [r for r in RUBROS if r != 'venta_real']

CUENTAS_N2  = {41,42,52,53,54,61,71,72,73,74}
UNIDADES    = {'CHILCO','INTERNACIONAL','OXIGENOS','CARGA','ADMINISTRACION',
               'BIG COLA','COPSERVIR','MULAS PROPIAS',
               'SERVICIO DE TRANSPORTE OPERATIVO','TRANSFERENCIAS OXIGENOS','LIQUIDOS'}

# Columnas de gastos de viaje a sumar
COLS_GASTOS_VIAJE = [
    'ALIMENTACION','HOSPEDAJE','MONTALLANTAS','OTROS GASTOS',
    'PARQUEOS','PEAJE AUTOMATICO','PROPINAS',
    'SUMINISTRO INSUMOS','TAXIS Y BUSES','VISITAS SEGURIDAD'
]

def is_placa(val):
    if pd.isna(val): return False
    return bool(re.match(r'^[A-Z]{2,3}\d{3,4}$', str(val).strip().upper()))

def map_rubro(n1, n2, n3, n4):
    n1 = str(n1).upper().strip() if n1 else ''
    n2 = str(n2).upper().strip() if n2 else ''
    n3 = str(n3).upper().strip() if n3 else ''
    n4 = str(n4).upper().strip() if n4 else ''
    if 'INGRESO' in n1 and 'OPERACIONAL' in n2 and 'DEVOLUCION' not in n3: return 'venta_real'
    if ('OPERACIONALES DE VENTAS' in n2 or 'OPERACIONALES DE ADMINISTRACION' in n2) and 'GASTOS DE PERSONAL' in n3: return 'nomina'
    if 'COMBUSTIBLE' in n4 or 'LUBRICANTE' in n4: return 'combustible'
    if 'MANTENIMIENTO' in n3 and ('FLOTA' in n4 or 'MAQUINARIA' in n4 or 'EQUIPO' in n4): return 'mtto'
    if 'OBLIGATORIO ACCIDENTE' in n4 or 'SOAT' in n4: return 'soat'
    if ('SEGUROS' in n3 and 'OBLIGATORIO' not in n4) or 'POLIZAS' in n4: return 'polizas'
    if 'TECNICOMECANICA' in n4 or 'REVISION TECNICO' in n4: return 'rtmc'
    if 'DE VEHICULOS' in n4 and 'IMPUESTO' in n3: return 'impuestos'
    if 'GASTOS DE VIAJE' in n3: return 'gastos_admi'
    if 'SERVICIOS' in n3 and ('TRANSPORTE' in n4 or 'FLETE' in n4): return 'bo'
    if 'DEPRECIACION' in n3 and 'FLOTA' in n4: return 'capital'
    return None

def clean(v):
    if v is None or (isinstance(v, float) and pd.isna(v)): return ''
    return str(v).strip()

def log(msg): print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def descargar(dbx, ruta):
    log(f"Descargando {ruta}...")
    _, res = dbx.files_download(ruta)
    log(f"  → {len(res.content)/1024:.0f} KB")
    return res.content

# ─── PROCESAR PRESUPUESTO ─────────────────────────────────────
def procesar_presupuesto(file_bytes):
    log("Procesando hoja Presupuesto...")
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name='Presupuesto', header=None)
    mes_start = {1:2,2:18,3:34,4:50,5:66,6:82,7:98,8:114,9:130,10:146,11:162,12:178}
    records = []
    for _, row in df.iloc[2:].iterrows():
        cc    = str(row[0]).strip() if pd.notna(row[0]) else ''
        placa = str(row[1]).strip() if pd.notna(row[1]) else ''
        if not cc or cc in ['nan',''] or not placa or placa in ['nan','']: continue
        for mes, start in mes_start.items():
            def v(offset):
                val = row[start+offset]
                if pd.isna(val): return 0
                try: return float(val)
                except: return 0
            records.append({
                'placa':placa,'cc':cc,'modelo':str(row[start]).strip(),'mes':mes,
                'venta':v(1),'gastos_admi':v(2),'bo':v(3),'mtto':v(4),
                'combustible':v(5),'sst':v(6),'nomina':v(7),'polizas':v(8),
                'impuestos':v(9),'soat':v(10),'rtmc':v(11),'avaluo':v(12),
                'capital':v(13),'total':v(14),
            })
    log(f"  → {len(records)} registros")
    return records

# ─── PROCESAR BASICA ──────────────────────────────────────────
def procesar_basica(file_bytes):
    log("Procesando hoja BASICA...")
    COLS = ['Periodo','nombre Unidad de Negocio','Cuenta_n2',
            'Nombre_cuenta_n1','Nombre_cuenta_n2','Nombre_cuenta_n3','Nombre_cuenta_n4',
            'Nombre_auxiliar','Centro de Costo','Nombre Centro de Costo',
            'Nombre Tercero','Notas','Movto_libro2']
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name='BASICA', header=0,
                       skiprows=5, usecols=COLS,
                       dtype={'Centro de Costo':str,'Movto_libro2':float,'Cuenta_n2':float})
    df['periodo_str'] = df['Periodo'].astype(str).str.strip()
    df = df[df['periodo_str'].str.startswith('2026')]
    df['mes_num'] = df['periodo_str'].str[4:].astype(int)
    df = df[df['Cuenta_n2'].isin(CUENTAS_N2)]
    df['un_upper'] = df['nombre Unidad de Negocio'].str.strip().str.upper()
    df = df[df['un_upper'].isin(UNIDADES)]
    df = df[df['Movto_libro2'].fillna(0) != 0]
    df['cc_presup'] = df['un_upper'].map(UN_TO_CC)
    df['placa']     = df['Centro de Costo'].apply(lambda x: str(x).strip().upper() if is_placa(x) else None)
    df['rubro']     = df.apply(lambda r: map_rubro(
        r['Nombre_cuenta_n1'],r['Nombre_cuenta_n2'],
        r['Nombre_cuenta_n3'],r.get('Nombre_cuenta_n4','')), axis=1)
    real = df[df['placa'].notna() & df['rubro'].notna() & df['cc_presup'].notna()].copy()
    log(f"  → {len(real):,} filas útiles")

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

    # Árbol cascada
    tree_raw = {}
    for _, row in real.iterrows():
        key = f"{row['placa']}|{int(row['mes_num'])}|{row['rubro']}"
        if key not in tree_raw: tree_raw[key] = {}
        n2=clean(row['Nombre_cuenta_n2']); n3=clean(row['Nombre_cuenta_n3'])
        aux=clean(row['Nombre_auxiliar']) or '(sin auxiliar)'
        ter=clean(row['Nombre Tercero']) or '(sin tercero)'
        nota=clean(row['Notas']); val=abs(float(row['Movto_libro2']))
        t=tree_raw[key]
        if n2 not in t: t[n2]={'_val':0,'_children':{}}
        t[n2]['_val']+=val
        c2=t[n2]['_children']
        if n3 not in c2: c2[n3]={'_val':0,'_children':{}}
        c2[n3]['_val']+=val
        c3=c2[n3]['_children']
        if aux not in c3: c3[aux]={'_val':0,'_children':{}}
        c3[aux]['_val']+=val
        c4=c3[aux]['_children']
        if ter not in c4: c4[ter]={'_val':0,'_notes':[]}
        c4[ter]['_val']+=val
        if nota: c4[ter]['_notes'].append({'n':nota[:80],'v':round(val)})

    def optimize(node, is_gadmi=False):
        r={'v':round(node.get('_val',0))}
        ch=node.get('_children',{})
        if ch:
            items=sorted(ch.items(),key=lambda x:-x[1].get('_val',0))[:20]
            r['c']={k[:60]:optimize(v,is_gadmi) for k,v in items}
        notes=node.get('_notes',[])
        if notes and not is_gadmi:
            acc={}
            for n in notes: acc[n['n']]=acc.get(n['n'],0)+n['v']
            r['n']=[{'t':t,'v':v} for t,v in sorted(acc.items(),key=lambda x:-x[1])[:5] if t]
        return r

    tree={}
    for key,node in tree_raw.items():
        rubro=key.split('|')[2]
        tree[key]={n2[:60]:optimize(n2d,rubro=='gastos_admi') for n2,n2d in node.items()}

    meses_reales=sorted(set(r['mes'] for r in real_records))
    log(f"  → {len(real_records)} registros pivotados, meses: {meses_reales}")
    return real_records, tree, meses_reales

# ─── PROCESAR VIAJES CLOUDFLEET ───────────────────────────────
def procesar_viajes(file_bytes):
    log("Procesando consolidado de viajes CloudFleet...")

    # Skiprows=4 — las primeras 4 filas son encabezado
    df = pd.read_excel(io.BytesIO(file_bytes), header=4, skiprows=0)
    # Alternativamente si header=4 no funciona
    if 'Fecha Salida' not in df.columns and 'Fecha_Salida' not in df.columns:
        df = pd.read_excel(io.BytesIO(file_bytes), skiprows=4)

    log(f"  → {len(df):,} filas, columnas: {df.columns.tolist()[:10]}...")

    # Normalizar nombres de columnas
    df.columns = [str(c).strip() for c in df.columns]

    # Columna fecha de salida
    col_fecha = next((c for c in df.columns if 'fecha' in c.lower() and 'sal' in c.lower()), None)
    col_placa  = next((c for c in df.columns if 'veh' in c.lower() or 'plac' in c.lower()), None)
    col_origen = next((c for c in df.columns if 'origen' in c.lower()), None)
    col_med_rec= next((c for c in df.columns if 'recorr' in c.lower()), None)
    col_med_sal= next((c for c in df.columns if 'medici' in c.lower() and 'sal' in c.lower()), None)
    col_med_lleg=next((c for c in df.columns if 'medici' in c.lower() and ('lleg' in c.lower() or 'arr' in c.lower())), None)

    log(f"  → Columnas detectadas:")
    log(f"     Fecha salida:  {col_fecha}")
    log(f"     Vehículo:      {col_placa}")
    log(f"     Origen:        {col_origen}")
    log(f"     Med. Recorrida:{col_med_rec}")
    log(f"     Med. Salida:   {col_med_sal}")
    log(f"     Med. Llegada:  {col_med_lleg}")

    # Filtrar solo LINDE y CHILCO en Origen
    # LINDE → PRAXAIR, CHILCO → CHILCO
    if col_origen:
        mask = df[col_origen].astype(str).str.upper().str.contains('LINDE|CHILCO', na=False)
        df = df[mask].copy()
        df['cc_viaje'] = df[col_origen].astype(str).str.upper().apply(
            lambda x: 'PRAXAIR' if 'LINDE' in x else ('CHILCO' if 'CHILCO' in x else None)
        )
        df = df[df['cc_viaje'].notna()]
    log(f"  → {len(df):,} filas después de filtro LINDE/CHILCO")

    # Parsear fecha y extraer mes
    if col_fecha:
        df[col_fecha] = pd.to_datetime(df[col_fecha], errors='coerce', dayfirst=True)
        df = df[df[col_fecha].dt.year == 2026]
        df['mes'] = df[col_fecha].dt.month
    log(f"  → {len(df):,} filas del año 2026")

    if len(df) == 0:
        log("  → Sin datos de viajes para 2026")
        return []

    # Extraer placa del campo vehículo
    if col_placa:
        df['placa_viaje'] = df[col_placa].astype(str).str.extract(r'([A-Z]{2,3}\d{3,4})', expand=False)

    # Columnas de gastos disponibles
    gastos_disponibles = [c for c in COLS_GASTOS_VIAJE if c in df.columns]
    log(f"  → Columnas de gastos encontradas: {gastos_disponibles}")

    # Convertir numéricas
    for col in [col_med_rec, col_med_sal, col_med_lleg] + gastos_disponibles:
        if col and col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Agrupar por placa + mes
    viajes_records = []
    placas = df['placa_viaje'].dropna().unique() if 'placa_viaje' in df.columns else []

    for placa in placas:
        df_p = df[df['placa_viaje'] == placa]
        for mes in df_p['mes'].dropna().unique():
            df_pm = df_p[df_p['mes'] == mes].sort_values(col_fecha) if col_fecha else df_p

            # 1. Km a cobrar = suma de Medición Recorrida
            km_cobrar = float(df_pm[col_med_rec].sum()) if col_med_rec else 0

            # 2. Km reales = último Medición Llegada - primer Medición Salida
            km_reales = 0
            if col_med_lleg and col_med_sal:
                ultimo_llegada = df_pm[col_med_lleg].replace(0, pd.NA).dropna()
                primer_salida  = df_pm[col_med_sal].replace(0, pd.NA).dropna()
                if len(ultimo_llegada) > 0 and len(primer_salida) > 0:
                    km_reales = float(ultimo_llegada.iloc[-1]) - float(primer_salida.iloc[0])

            # 3. Gastos de viaje
            gastos_viaje = sum(float(df_pm[c].sum()) for c in gastos_disponibles if c in df_pm.columns)

            # 4. CC
            cc = df_pm['cc_viaje'].iloc[0] if 'cc_viaje' in df_pm.columns else ''

            viajes_records.append({
                'placa':      placa,
                'mes':        int(mes),
                'cc':         cc,
                'km_cobrar':  round(km_cobrar, 1),
                'km_reales':  round(km_reales, 1),
                'gastos_viaje': round(gastos_viaje, 0),
                'num_viajes': int(len(df_pm)),
            })

    log(f"  → {len(viajes_records)} registros de viajes procesados")
    return viajes_records

# ─── MAIN ─────────────────────────────────────────────────────
def main():
    log("="*55)
    log("CCM Fleet Budget — Procesador de datos")
    log("="*55)

    try:
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=REFRESH_TOKEN,
            app_key=APP_KEY, app_secret=APP_SECRET
        )
        cuenta = dbx.users_get_current_account()
        log(f"Dropbox: {cuenta.name.display_name}")
    except Exception as e:
        log(f"ERROR Dropbox: {e}"); sys.exit(1)

    try:
        presup_bytes = descargar(dbx, DROPBOX_PRESUPUESTO)
        basica_bytes = descargar(dbx, DROPBOX_BASICA)
        viajes_bytes = descargar(dbx, DROPBOX_VIAJES)
    except Exception as e:
        log(f"ERROR descargando: {e}"); sys.exit(1)

    try:
        presup_records              = procesar_presupuesto(presup_bytes)
        real_records, tree, meses_r = procesar_basica(basica_bytes)
        viajes_records              = procesar_viajes(viajes_bytes)
    except Exception as e:
        log(f"ERROR procesando: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    data = {
        'presupuesto':  presup_records,
        'real':         real_records,
        'tree':         tree,
        'meses_reales': meses_r,
        'viajes':       viajes_records,
        'last_update':  datetime.now().isoformat(),
    }

    json_bytes = json.dumps(data, ensure_ascii=False, separators=(',',':')).encode('utf-8')
    log(f"JSON generado: {len(json_bytes)/1024:.0f} KB")

    try:
        dbx.files_upload(json_bytes, DROPBOX_JSON_OUT,
                         mode=dropbox.files.WriteMode.overwrite)
        log("✓ JSON subido a Dropbox")
    except Exception as e:
        log(f"ERROR subiendo: {e}"); sys.exit(1)

    log("="*55)
    log("✓ Proceso completado")
    log(f"  Presupuesto: {len(presup_records)} registros")
    log(f"  Real:        {len(real_records)} registros")
    log(f"  Viajes:      {len(viajes_records)} registros")
    log(f"  Meses reales:{meses_r}")
    log("="*55)

if __name__ == '__main__':
    main()
