from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import io
from datetime import datetime

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

column_corrector = 21
last_excel_stream = None  # memoria temporal

@app.get("/heartbeat")
def root():
    return {"status": "ok", "message": "ZuCo API is running 🚐"}

@app.get("/", response_class=HTMLResponse)
async def serve_index(request: Request):
    with open("templates/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.post("/process")
async def process_files(
    fleet_file: UploadFile = File(...),
    disponibilidad_file: UploadFile = File(...)
):
    global last_excel_stream

    fleet_bytes = await fleet_file.read()
    disp_bytes = await disponibilidad_file.read()

    fleet_df = pd.read_excel(io.BytesIO(fleet_bytes))
    disp_df = pd.read_excel(io.BytesIO(disp_bytes))

    fleet_df.columns = fleet_df.columns.str.strip().str.upper()
    disp_df.columns = disp_df.columns.str.strip().str.upper()

    if 'PLACA' not in fleet_df.columns or 'ESTADO' not in fleet_df.columns:
        return JSONResponse({"error": "Missing 'Placa' or 'Estado' column in fleet file."}, status_code=400)
    if 'VEÍCULO' not in disp_df.columns:
        return JSONResponse({"error": "Missing 'Veículo' column in availability file."}, status_code=400)

    fleet_df = fleet_df[['PLACA', 'ESTADO']].dropna()
    fleet_df['PLACA'] = fleet_df['PLACA'].astype(str).str.upper().str.strip()
    disp_set = set(disp_df['VEÍCULO'].dropna().astype(str).str.upper().str.strip())

    activos = set(fleet_df[fleet_df['ESTADO'] == 'ATIVO - BIPANDO']['PLACA'])
    ociosos = set(fleet_df[fleet_df['ESTADO'] == 'FROTA OCIOSA']['PLACA'])

    activos_deberian_ser_ociosos = activos & disp_set
    ociosos_deberian_ser_activos = ociosos - disp_set

    output_df = pd.DataFrame(columns=['Dominio'] + [''] * column_corrector + ['Estado'])
    for placa in activos_deberian_ser_ociosos:
        output_df.loc[len(output_df)] = [placa] + [''] * column_corrector + ['FROTA OCIOSA']
    for placa in ociosos_deberian_ser_activos:
        output_df.loc[len(output_df)] = [placa] + [''] * column_corrector + ['ATIVO - BIPANDO']

    total_flota = len(fleet_df)
    estado_counts = fleet_df['ESTADO'].value_counts().to_dict() if total_flota else {}
    estado_pct = {k: round(v * 100 / total_flota, 2) for k, v in estado_counts.items()} if total_flota else {}

    table_data = [{"Estado": k, "Cantidad": v, "Porcentaje": estado_pct.get(k, 0)} for k, v in estado_counts.items()]

    # Save Excel in memory for /download
    excel_stream = io.BytesIO()
    output_df.to_excel(excel_stream, index=False)
    excel_stream.seek(0)
    last_excel_stream = excel_stream

    current_date = datetime.now().strftime("%d%m%Y")
    filename = f"vehicle_fleet_update_{current_date}.xlsx"

    return {
        "status": "success",
        "table_data": table_data,
        "filename": filename
    }

@app.get("/download")
async def download_excel():
    global last_excel_stream
    if last_excel_stream is None:
        return JSONResponse({"error": "No Excel file ready for download."}, status_code=400)

    current_date = datetime.now().strftime("%d%m%Y")
    filename = f"vehicle_fleet_update_{current_date}.xlsx"

    return StreamingResponse(
        last_excel_stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
