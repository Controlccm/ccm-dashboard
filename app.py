import os, json, time, threading, logging
from datetime import datetime
import dropbox
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

APP_KEY       = os.environ.get('DROPBOX_APP_KEY',       'i3qh1or39zreiih')
APP_SECRET    = os.environ.get('DROPBOX_APP_SECRET',    'tzqnzdw1xvwwnwg')
REFRESH_TOKEN = os.environ.get('DROPBOX_REFRESH_TOKEN', 'hQTVhFF7Oa4AAAAAAAAAAWPGkeIttW-BgqwmcC3QF_9vw7q8vcDk1h1SlqDiA1-5')
DROPBOX_JSON    = os.environ.get('DROPBOX_JSON',    '/ccm_data.json')
REFRESH_MINUTES = int(os.environ.get('REFRESH_MINUTES', '30'))

_cache = {'data': None, 'last_update': None, 'error': None}

def get_dbx():
    return dropbox.Dropbox(
        oauth2_refresh_token=REFRESH_TOKEN,
        app_key=APP_KEY,
        app_secret=APP_SECRET
    )

def download_json():
    global _cache
    try:
        log.info(f"Descargando JSON: {DROPBOX_JSON}")
        dbx = get_dbx()
        _, res = dbx.files_download(DROPBOX_JSON)
        data = json.loads(res.content.decode('utf-8'))
        _cache['data']        = data
        _cache['last_update'] = datetime.now().isoformat()
        _cache['error']       = None
        n_presup = len(data.get('presupuesto', []))
        n_real   = len(data.get('real', []))
        log.info(f"Cache actualizado — {n_presup} presup, {n_real} real")
    except Exception as e:
        _cache['error'] = str(e)
        log.error(f"Error descargando JSON: {e}")

def scheduler():
    while True:
        time.sleep(REFRESH_MINUTES * 60)
        download_json()

# ── Descarga inicial SINCRONA antes de arrancar ──────────────
log.info("Carga inicial...")
download_json()
log.info(f"Cache listo: {_cache['data'] is not None}, error: {_cache['error']}")

# Scheduler en background para refrescar periódicamente
_t = threading.Thread(target=scheduler, daemon=True)
_t.start()

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/data')
def get_data():
    if _cache['data'] is None:
        return jsonify({'error': _cache['error'] or 'Sin datos'}), 503
    return jsonify(_cache['data'])

@app.route('/api/status')
def status():
    return jsonify({
        'ok':            _cache['data'] is not None,
        'last_update':   _cache['last_update'],
        'error':         _cache['error'],
        'refresh_every': f'{REFRESH_MINUTES} minutos',
    })

@app.route('/api/refresh', methods=['POST'])
def force_refresh():
    threading.Thread(target=download_json, daemon=True).start()
    return jsonify({'message': 'Actualizacion iniciada'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
