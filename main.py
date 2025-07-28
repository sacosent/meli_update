from fastapi                import FastAPI, UploadFile, File, Request
from fastapi.staticfiles    import StaticFiles
from fastapi.responses      import StreamingResponse, HTMLResponse
import pandas as pd
import io
import os

app = FastAPI()

app.mount("/static", StaticFiles(directory="templates"), name="static")

@app.get("/heartbeat")
def root():
    return {"status": "ok", "message": "La API de actualización de vehículos está funcionando correctamente 🚐"}

@app.get("/", response_class=HTMLResponse)
async def serve_index(request: Request):
    with open("templates/index.html") as f:
        return HTMLResponse(f.read())

@app.post("/procesar")
async def procesar_excel(
    fleet_file: UploadFile = File(...),
    disponibilidad_file: UploadFile = File(...),
):
    # Leer los archivos recibidos
    fleet_content = await fleet_file.read()
    disp_content = await disponibilidad_file.read()

    fleet_df = pd.read_excel(io.BytesIO(fleet_content))
    disponibilidad_df = pd.read_excel(io.BytesIO(disp_content))

    # Normalizar y filtrar
    fleet_filtered = fleet_df[['Placa', 'Estado']].copy()
    fleet_filtered['Placa'] = fleet_filtered['Placa'].astype(str).str.upper().str.strip()
    disponibilidad_placas = disponibilidad_df['Veículo'].astype(str).str.upper().str.strip().unique()

    # Condición A
    cond_a = fleet_filtered[
        (fleet_filtered['Estado'] == "ATIVO - BIPANDO") &
        (fleet_filtered['Placa'].isin(disponibilidad_placas))
    ].copy()
    cond_a['Estado'] = "FROTA OCIOSA"

    # Condición B
    cond_b = fleet_filtered[
        (fleet_filtered['Estado'] == "FROTA OCIOSA") &
        (~fleet_filtered['Placa'].isin(disponibilidad_placas))
    ].copy()
    cond_b['Estado'] = "ATIVO - BIPANDO"

    # Unir resultados
    result_df = pd.concat([cond_a, cond_b], ignore_index=True)
    output_df = pd.DataFrame(columns=["Dominio", "Estado"])
    output_df['Dominio'] = result_df['Placa'].values
    output_df['Estado'] = result_df['Estado'].values

    # Guardar a memoria
    output_stream = io.BytesIO()
    with pd.ExcelWriter(output_stream, engine='openpyxl') as writer:
        output_df.to_excel(writer, index=False)
    output_stream.seek(0)

    # Devolver archivo como descarga
    return StreamingResponse(output_stream, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={
        "Content-Disposition": "attachment; filename=Vehiculos_para_actualizar_estado.xlsx"
    })
