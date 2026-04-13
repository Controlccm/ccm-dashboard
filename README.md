# CCM Fleet Budget Dashboard

Dashboard de presupuesto y gastos reales de flota vehicular.

## Arquitectura
```
Dropbox (Excel) → Render (Python/Flask API) → Dashboard (HTML estático)
```

## Variables de entorno requeridas en Render

| Variable | Descripción | Ejemplo |
|---|---|---|
| `DROPBOX_TOKEN` | Token de acceso Dropbox | `sl.u.XXXX...` |
| `DROPBOX_FILE_PATH` | Ruta del Excel en Dropbox | `/Presupuesto.xlsx` |
| `REFRESH_MINUTES` | Frecuencia de actualización | `120` |

## Despliegue en Render

1. Sube este repositorio a GitHub
2. En Render → New → Web Service → conecta el repo
3. Configura las variables de entorno
4. Deploy

## Endpoints API

- `GET /` — Dashboard principal
- `GET /api/data` — Datos completos (presupuesto + real + árbol)
- `GET /api/status` — Estado del servidor y última actualización
- `POST /api/refresh` — Forzar actualización inmediata desde Dropbox

## Estructura del proyecto
```
ccm-dashboard/
├── app.py              # Servidor Flask
├── requirements.txt    # Dependencias
├── render.yaml         # Config Render
└── static/
    └── index.html      # Dashboard HTML
```
