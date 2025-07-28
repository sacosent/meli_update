from fastapi                import FastAPI, UploadFile, File, Request
from fastapi.staticfiles    import StaticFiles
from fastapi.responses      import HTMLResponse, JSONResponse
import pandas as pd
import io
import tempfile
import os

column_corrector = 21

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
async def procesar_archivos(
    fleet_file: UploadFile = File(...),
    disponibilidad_file: UploadFile = File(...)
):
    try:
        fleet_bytes = await fleet_file.read()
        disp_bytes = await disponibilidad_file.read()

        fleet_df = pd.read_excel(io.BytesIO(fleet_bytes))
        disp_df = pd.read_excel(io.BytesIO(disp_bytes))

        # Normalización de columnas
        fleet_df.columns = fleet_df.columns.str.strip().str.upper()
        disp_df.columns = disp_df.columns.str.strip().str.upper()

        # Validación de columnas
        if 'PLACA' not in fleet_df.columns or 'ESTADO' not in fleet_df.columns:
            return JSONResponse({"error": "Las columnas 'Placa' y/o 'Estado' no se encontraron en fleet-moviles."}, status_code=400)
        if 'VEÍCULO' not in disp_df.columns:
            return JSONResponse({"error": "La columna 'Veículo' no se encontró en disponibilidad."}, status_code=400)

        # Preprocesamiento
        fleet_df = fleet_df[['PLACA', 'ESTADO']].dropna()
        fleet_df['PLACA'] = fleet_df['PLACA'].astype(str).str.upper().str.strip()
        disp_set = set(disp_df['VEÍCULO'].dropna().astype(str).str.upper().str.strip())

        # Identificación de inconsistencias
        activos = set(fleet_df[fleet_df['ESTADO'] == 'ATIVO - BIPANDO']['PLACA'])
        ociosos = set(fleet_df[fleet_df['ESTADO'] == 'FROTA OCIOSA']['PLACA'])

        activos_deberian_ser_ociosos = activos & disp_set
        ociosos_deberian_ser_activos = ociosos - disp_set

        # Creación del Excel final
        output_df = pd.DataFrame(columns=['Dominio'] + [''] * column_corrector + ['Estado'])
        for placa in activos_deberian_ser_ociosos:
            output_df.loc[len(output_df)] = [placa] + [''] * column_corrector + ['FROTA OCIOSA']
        for placa in ociosos_deberian_ser_activos:
            output_df.loc[len(output_df)] = [placa] + [''] * column_corrector + ['ATIVO - BIPANDO']

        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            output_path = tmp.name
        output_df.to_excel(output_path, index=False)


        total_flota = len(fleet_df)
        if total_flota == 0:
            estado_counts = {}
            estado_pct = {}
        else:
            estado_counts = fleet_df['ESTADO'].value_counts().to_dict()
            estado_pct = {k: round(v * 100 / total_flota, 2) for k, v in estado_counts.items()}


        # Save Excel and build download link
        file_name = os.path.basename(output_path)
        os.rename(output_path, f'static/{file_name}')
        download_link = f"/static/{file_name}"

        return JSONResponse({
            "status": "success",
            "table_data": [{"Estado": k, "Cantidad": v, "Porcentaje": estado_pct.get(k, 0)} for k, v in estado_counts.items()],
            "download_link": download_link
        })

    
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)