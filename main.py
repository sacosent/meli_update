from fastapi                import FastAPI, UploadFile, File, Request
from fastapi.staticfiles    import StaticFiles
from fastapi.responses      import HTMLResponse, JSONResponse
import pandas as pd
import io
import base64

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/heartbeat")
def root():
    return {"status": "ok", "message": "La API de actualización de vehículos está funcionando correctamente 🚐"}

@app.get("/", response_class=HTMLResponse)
async def serve_index(request: Request):
    with open("templates/index.html", "r", encoding="utf-8") as f:

        return HTMLResponse(f.read())

@app.post("/process")
async def procesar_archivos(fleet_file: UploadFile = File(...), disponibilidad_file: UploadFile = File(...)):
    fleet_df = pd.read_excel(io.BytesIO(await fleet_file.read()))
    disp_df = pd.read_excel(io.BytesIO(await disponibilidad_file.read()))

    fleet_df = fleet_df[['Placa', 'Estado']].dropna()
    fleet_df['Placa'] = fleet_df['Placa'].astype(str).str.upper().str.strip()
    disp_set = set(disp_df['Veículo'].dropna().astype(str).str.upper().str.strip())

    activos = fleet_df[fleet_df['Estado'] == 'ATIVO - BIPANDO']
    ociosos = fleet_df[fleet_df['Estado'] == 'FROTA OCIOSA']

    activos_mal = activos[activos['Placa'].isin(disp_set)]
    ociosos_mal = ociosos[~ociosos['Placa'].isin(disp_set)]

    total_flota = len(fleet_df)
    total_activos = len(activos)
    total_ociosos = len(ociosos)
    cant_activos_mal = len(activos_mal)
    cant_ociosos_mal = len(ociosos_mal)
    total_errores = cant_activos_mal + cant_ociosos_mal

    pct_activos = round((total_activos / total_flota) * 100, 2) if total_flota else 0
    pct_ociosos = round((total_ociosos / total_flota) * 100, 2) if total_flota else 0
    pct_activos_mal = round((cant_activos_mal / total_flota) * 100, 2) if total_flota else 0
    pct_ociosos_mal = round((cant_ociosos_mal / total_flota) * 100, 2) if total_flota else 0
    pct_total_errores = round((total_errores / total_flota) * 100, 2) if total_flota else 0

    # Estados detallados
    estado_counts = fleet_df['Estado'].value_counts()
    estados_detalle = {
        estado: f"{count} ({round((count / total_flota) * 100, 2)}%)"
        for estado, count in estado_counts.items()
    }

    # Create output Excel
    modelo_df = pd.DataFrame(columns=list("ABCDEFGHIJKLMNOPQRSTUVWX"))
    modelo_df['A'] = pd.concat([activos_mal['Placa'], ociosos_mal['Placa']], ignore_index=True)
    modelo_df['W'] = ['FROTA OCIOSA'] * cant_activos_mal + ['ATIVO - BIPANDO'] * cant_ociosos_mal

    output = io.BytesIO()
    modelo_df.to_excel(output, index=False, header=False)
    output.seek(0)
    b64_excel = base64.b64encode(output.read()).decode('utf-8')

    # Build response
    return JSONResponse({
        "file": b64_excel,
        "stats": {
            "Total Flota": total_flota,
            **estados_detalle,
            "Activas mal": f"{cant_activos_mal} ({pct_activos_mal}%)",
            "Ociosas mal": f"{cant_ociosos_mal} ({pct_ociosos_mal}%)",
            "Total con errores": f"{total_errores} ({pct_total_errores}%)"
        },
        "chart1": {
            "labels": list(estados_detalle.keys()),
            "values": [int(s.split()[0]) for s in estados_detalle.values()]
        },
        "chart2": {
            "labels": ["Activas que deberían ser ociosas", "Ociosas que deberían ser activas"],
            "values": [cant_activos_mal, cant_ociosos_mal]
        }
    })