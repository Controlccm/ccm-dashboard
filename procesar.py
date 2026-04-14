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
DROPBOX_TOKEN       = "sl.u.AGZYQCpbwd8YTzJXNtr8L58nDEVNu_ljHTmlCfz9vpehoonwdimrAmOr9_-D01rEYnsug8FzyprJIDSdETfWFGcd0c2hoCCrgUEAw4mpGuTcSZo9Inrw5pHLpmSovvK5arMOTPhmX-jOANv-yFK8Zb1Pcc0HLDslPOFikUh5ImJQBPSShq2hjC7Q-BQHl3VVlBRiWKiMklxNgmK-NVQoTpuXXFdtmlCG6jCAwajHtPd3EkOB8OXEW014y-KZ0MH2Eh1zqk6pSlUeWm6h39Y7ZGAiAX2hTtbKJGJpVoeMxIgfBswQqPhsW79PF9Bk8_nUFRZgU_S3sT_eDm6VuXeYq0y_oyzMivw3c-zdr4VTpvPQKdYae_Zln3z1oeedJ0HOwmLEXSxrt1BDDU3zDAQKiyBhBkfV5yptPE6d6dn2g8LYB_RdpaU0XeoY570iIq2vWugppksYU7rG-ndzXF6nVkoibgrTHCBMWio4AL8tLq9By3FG_lh-kt2XxcJAI61IhG-u8EPlw3Y3aDQ2ue8R-yX9i9_9Vsa8BKtArfRJdPsSOI_4e-LmUreWJ37Ry-Ra9hiffpFT4uABZi8_TjDQUG6YxLUxxnLQSsZl5TYzrzrzkSKFrl8G2JCrvTi2DMjB0FmsLhFhcqFbK4pcTUbYeA7FSy7BsW5DqfDlErO2Zg9B5a7sk5QWKIlek1FhFcrEO8n2-dWjZfccx-F_rUnk0fVdi1ip0ayxWljXQlY1WC8zasD4KVX78f0CiONLrQFke_ynSwPEJojOn-5Uf_ob-3qKUJwARkenv9KTxDot-EHmUUMcrsr0xAR-ek6wdaonU9g7cx07G3VJ7bOmyt7GT9CuxqD52giyblvlfIWskg6BkLZxN4azICssiCYr6csE7tgSPv7uVybvzXVZVDJiYEfp_1gYqYUrMzG5exnayYGqpNsgSRSI0_nD9_3XHd9_B3u3OAPYNLZ4YyE6vgBZxsbke5jO_Z03wGG8DezI_FZY3pD-tLSE0q7MpvyVjn0RrFBba8Jm3ppLSejVmYZOWbNXU95kSmkN0zHWUHqCgJ9cCYG9QcIMgT6hcqK1BaX119vTtVMqlOJU6p7NattYojFpAhOrg8uHtvgCJyOleLjtuq9UGtfRf2koUk0fVXLP82Yfu7WkQYhTlp_wkjJ5h-wQzHFblwmb7wh32TdFFq1TGJZwvO6y3IcUAFa9NjUxWWRaKgrShhhi-mrCxXPjLBfKRfwdcS6n90DS1LDXqw_obzQ7RJWmsHX098Y5xa9V9DcThn-Hqc5YXnriDLesEP15xtvyyRZxFSpCB-5_1IKr6t3yUxnYvYHZJj1YNUCbzGQ"

DROPBOX_PRESUPUESTO = "/Presupuesto.xlsx"
DROPBOX_BASICA      = "/Nueva/BASICA 2026.xlsx"
DROPBOX_JSON_OUT    = "/ccm_data.json"   # donde se guarda el JSON procesado

# ─── CONSTANTES ───────────────────────────────────────────────
CUENTAS_N2 = {41, 42, 52, 53, 54, 61, 71, 72, 73, 74}

UNIDADES = {
    'CHILCO', 'INTERNACIONAL', 'OXIGENOS', 'CARGA', 'ADMINISTRACION',
    'BIG COLA', 'COPSERVIR', 'MULAS PROPIAS',
    'SERVICIO DE TRANSPORTE OPERATIVO', 'TRANSFERENCIAS OXIGENOS', 'LIQUIDOS'
}

UN_TO_CC = {
    'ADMINISTRACION': 'ADMINISTRATIVO', 'ADMINISTRACIÓN': 'ADMINISTRATIVO',
    'ADMON': 'ADMINISTRATIVO',
    'CHILCO': 'CHILCO',
    'COPSERVIR': 'COOPSERVIR',
    'MULAS PROPIAS': 'MULAS PROPIAS',
    'OXIGENOS': 'PRAXAIR', 'OXÍGENOS': 'PRAXAIR',
    'TRANSFERENCIAS OXIGENOS': 'PRAXAIR',
    'CARGA': 'CARGA',
    'BIG COLA': 'BIG COLA',
    'INTERNACIONAL': 'INTERNACIONAL',
    'SERVICIO DE TRANSPORTE OPERATIVO': 'SERVICIO DE TRANSPORTE OPERATIVO',
    'LIQUIDOS': 'LIQUIDOS',
}

RUBROS      = ['venta_real','nomina','combustible','mtto','gastos_admi','bo','polizas','soat','rtmc','impuestos','capital']
GASTO_RUBROS= [r for r in RUBROS if r != 'venta_real']

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def is_placa(val):
    if not val or str(val).strip() in ('nan','None',''): return False
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

# ─── DESCARGA DESDE DROPBOX ───────────────────────────────────
def descargar(dbx, ruta):
    log(f"Descargando {ruta}...")
    _, res = dbx.files_download(ruta)
    log(f"  → {len(res.content)/1024:.0f} KB descargados")
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
    log(f"  → {len(records)} registros de presupuesto")
    return records

# ─── PROCESAR BASICA ──────────────────────────────────────────
def procesar_basica(file_bytes):
    log("Procesando hoja BASICA...")
    COLS = [
        'Periodo', 'nombre Unidad de Negocio', 'Cuenta_n2',
        'Nombre_cuenta_n1', 'Nombre_cuenta_n2', 'Nombre_cuenta_n3', 'Nombre_cuenta_n4',
        'Nombre_auxiliar', 'Centro de Costo', 'Nombre Centro de Costo',
        'Nombre Tercero', 'Notas', 'Movto_libro2'
    ]
    df = pd.read_excel(
        io.BytesIO(file_bytes), sheet_name='BASICA', header=0,
        usecols=COLS, dtype={'Centro de Costo': str, 'Movto_libro2': float, 'Cuenta_n2': float}
    )
    log(f"  → {len(df):,} filas cargadas, aplicando filtros...")

    # Filtros
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
        r['Nombre_cuenta_n1'], r['Nombre_cuenta_n2'],
        r['Nombre_cuenta_n3'], r.get('Nombre_cuenta_n4', '')), axis=1)

    real = df[df['placa'].notna() & df['rubro'].notna() & df['cc_presup'].notna()].copy()
    log(f"  → {len(real):,} filas útiles tras filtros")

    # Pivot por placa + mes
    pivoted = {}
    for _, row in real.iterrows():
        key = (row['placa'], int(row['mes_num']))
        if key not in pivoted:
            pivoted[key] = {
                'placa': row['placa'], 'mes': int(row['mes_num']),
                'unidad_negocio': clean(row['nombre Unidad de Negocio']),
                'cc': clean(row['cc_presup']),
                'cc_nombre': clean(row['Nombre Centro de Costo'])
            }
            for rb in RUBROS: pivoted[key][rb] = 0.0
        pivoted[key][row['rubro']] = pivoted[key].get(row['rubro'], 0.0) + abs(float(row['Movto_libro2']))

    real_records = list(pivoted.values())
    for r in real_records:
        r['total_real'] = sum(r.get(rb, 0) for rb in GASTO_RUBROS)
    log(f"  → {len(real_records)} registros reales pivotados")

    # Árbol cascada N2 → N3 → Auxiliar → Tercero → Notas
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
        if n2 not in t: t[n2] = {'_val': 0, '_children': {}}
        t[n2]['_val'] += val
        c2 = t[n2]['_children']
        if n3 not in c2: c2[n3] = {'_val': 0, '_children': {}}
        c2[n3]['_val'] += val
        c3 = c2[n3]['_children']
        if aux not in c3: c3[aux] = {'_val': 0, '_children': {}}
        c3[aux]['_val'] += val
        c4 = c3[aux]['_children']
        if ter not in c4: c4[ter] = {'_val': 0, '_notes': []}
        c4[ter]['_val'] += val
        if nota: c4[ter]['_notes'].append({'n': nota[:80], 'v': round(val)})

    def optimize(node, is_gadmi=False):
        r = {'v': round(node.get('_val', 0))}
        ch = node.get('_children', {})
        if ch:
            items = sorted(ch.items(), key=lambda x: -x[1].get('_val', 0))[:20]
            r['c'] = {k[:60]: optimize(v, is_gadmi) for k, v in items}
        notes = node.get('_notes', [])
        if notes and not is_gadmi:
            acc = {}
            for n in notes:
                acc[n['n']] = acc.get(n['n'], 0) + n['v']
            r['n'] = [{'t': t, 'v': v} for t, v in sorted(acc.items(), key=lambda x: -x[1])[:5] if t]
        return r

    tree = {}
    for key, node in tree_raw.items():
        rubro = key.split('|')[2]
        tree[key] = {n2[:60]: optimize(n2d, rubro == 'gastos_admi') for n2, n2d in node.items()}

    meses_reales = sorted(set(r['mes'] for r in real_records))
    return real_records, tree, meses_reales

# ─── MAIN ─────────────────────────────────────────────────────
def main():
    log("=" * 50)
    log("CCM Fleet Budget — Procesador de datos")
    log("=" * 50)

    try:
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        cuenta = dbx.users_get_current_account()
        log(f"Conectado a Dropbox: {cuenta.name.display_name}")
    except Exception as e:
        log(f"ERROR: No se pudo conectar a Dropbox: {e}")
        sys.exit(1)

    # Descargar archivos
    try:
        presup_bytes = descargar(dbx, DROPBOX_PRESUPUESTO)
        basica_bytes = descargar(dbx, DROPBOX_BASICA)
    except Exception as e:
        log(f"ERROR descargando archivos: {e}")
        sys.exit(1)

    # Procesar
    try:
        presup_records = procesar_presupuesto(presup_bytes)
        real_records, tree, meses_reales = procesar_basica(basica_bytes)
    except Exception as e:
        log(f"ERROR procesando: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)

    # Construir JSON final
    data = {
        'presupuesto':  presup_records,
        'real':         real_records,
        'tree':         tree,
        'meses_reales': meses_reales,
        'last_update':  datetime.now().isoformat(),
    }

    json_bytes = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    log(f"JSON generado: {len(json_bytes)/1024:.0f} KB")

    # Subir a Dropbox
    try:
        log(f"Subiendo JSON a Dropbox: {DROPBOX_JSON_OUT}")
        dbx.files_upload(
            json_bytes,
            DROPBOX_JSON_OUT,
            mode=dropbox.files.WriteMode.overwrite
        )
        log("✓ JSON subido correctamente a Dropbox")
    except Exception as e:
        log(f"ERROR subiendo JSON: {e}")
        sys.exit(1)

    log("=" * 50)
    log(f"✓ Proceso completado exitosamente")
    log(f"  Presupuesto: {len(presup_records)} registros")
    log(f"  Real:        {len(real_records)} registros")
    log(f"  Meses reales: {meses_reales}")
    log(f"  Árbol:       {len(tree)} entradas")
    log("=" * 50)

if __name__ == '__main__':
    main()
