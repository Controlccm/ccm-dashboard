import os, io, json, time, threading, logging
from datetime import datetime
import dropbox
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

DROPBOX_TOKEN   = os.environ.get('DROPBOX_TOKEN', '')
DROPBOX_JSON    = os.environ.get('DROPBOX_JSON', '/ccm_data.json')
REFRESH_MINUTES = int(os.environ.get('REFRESH_MINUTES', '30'))

_cache = {'data': None, 'last_update': None, 'error': None}

def download_json():
    global _cache
    try:
        log.info(f"Descargando JSON: {DROPBOX_JSON}")
        dbx = dropbox.Dropbox(DROPBOX_TOKEN)
        _, res = dbx.files_download(DROPBOX_JSON)
        data = json.loads(res.content.decode('utf-8'))
        _cache['data']        = data
        _cache['last_update'] = datetime.now().isoformat()
        _cache['error']       = None
        log.info(f"JSON OK — {len(res.content)/1024:.0f} KB")
    except Exception as e:
        _cache['error'] = str(e)
        log.error(f"Error: {e}")

def scheduler():
    while True:
        download_json()
        time.sleep(REFRESH_MINUTES * 60)

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')

@app.route('/api/data')
def get_data():
    if _cache['data'] is None:
        return jsonify({'error': _cache['error'] or 'Cargando...'}), 503
    return jsonify(_cache['data'])

@app.route('/api/status')
def status():
    return jsonify({
        'ok': _cache['data'] is not None,
        'last_update': _cache['last_update'],
        'error': _cache['error'],
        'refresh_every': f'{REFRESH_MINUTES} minutos',
    })

@app.route('/api/refresh', methods=['POST'])
def force_refresh():
    threading.Thread(target=download_json, daemon=True).start()
    return jsonify({'message': 'Actualizacion iniciada'})

_t = threading.Thread(target=scheduler, daemon=True)
_t.start()
log.info("Scheduler iniciado")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
