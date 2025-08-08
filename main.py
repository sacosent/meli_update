from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import io
from datetime import datetime

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- Config ---
TEMPLATE_PATH = "./Planilla-Modelo.xlsx"   # asegúrate de tener este archivo en la raíz del proyecto
SHEET_NAME = "Worksheet"                   # nombre de la hoja del template
last_excel_stream = None                   # buffer en memoria para /download


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
    """
    - Lee los 2 Excels subidos
    - Calcula inconsistencias de estado
    - Construye un DataFrame con EXACTA estructura/orden de columnas de Planilla-Modelo.xlsx
    - Llena Dominio y Estado; deja el resto vacío
    - Guarda el Excel resultante en memoria (last_excel_stream) para /download
    - Devuelve el resumen (table_data) y filename
    """
    global last_excel_stream
    try:
        # Leer bytes
        fleet_bytes = await fleet_file.read()
        disp_bytes  = await disponibilidad_file.read()

        # DataFrames origen
        fleet_df = pd.read_excel(io.BytesIO(fleet_bytes))
        disp_df  = pd.read_excel(io.BytesIO(disp_bytes))

        # Normalizar columnas
        fleet_df.columns = fleet_df.columns.str.strip().str.upper()
        disp_df.columns  = disp_df.columns.str.strip().str.upper()

        # Validaciones mínimas
        if 'PLACA' not in fleet_df.columns or 'ESTADO' not in fleet_df.columns:
            return JSONResponse({"error": "Missing 'Placa' or 'Estado' column in fleet file."}, status_code=400)
        if 'VEÍCULO' not in disp_df.columns:
            return JSONResponse({"error": "Missing 'Veículo' column in availability file."}, status_code=400)

        # Preproceso
        fleet_df = fleet_df[['PLACA', 'ESTADO']].dropna()
        fleet_df['PLACA'] = fleet_df['PLACA'].astype(str).str.upper().str.strip()
        disp_set = set(disp_df['VEÍCULO'].dropna().astype(str).str.upper().str.strip())

        # Conjuntos por estado
        activos = set(fleet_df[fleet_df['ESTADO'] == 'ATIVO - BIPANDO']['PLACA'])
        ociosos = set(fleet_df[fleet_df['ESTADO'] == 'FROTA OCIOSA']['PLACA'])

        # Reglas de actualización
        activos_deberian_ser_ociosos = activos & disp_set       # están ativos pero deberían ser ociosos
        ociosos_deberian_ser_activos = ociosos - disp_set       # están ociosos pero deberían ser ativos

        # --- Estructura del template (mismas columnas/orden) ---
        tpl = pd.read_excel(TEMPLATE_PATH, sheet_name=SHEET_NAME, nrows=0)
        if isinstance(tpl, dict):  # por si cambian el SHEET_NAME y viene un dict
            tpl = next(iter(tpl.values()))
        template_cols = list(tpl.columns)

        if 'Dominio' not in template_cols or 'Estado' not in template_cols:
            return JSONResponse({"error": "Template must include 'Dominio' and 'Estado' columns."}, status_code=500)

        # DataFrame final con columnas del template
        output_rows = []

        for placa in sorted(activos_deberian_ser_ociosos):
            row = {col: "" for col in template_cols}
            row['Dominio'] = placa
            row['Estado'] = 'FROTA OCIOSA'
            output_rows.append(row)

        for placa in sorted(ociosos_deberian_ser_activos):
            row = {col: "" for col in template_cols}
            row['Dominio'] = placa
            row['Estado'] = 'ATIVO - BIPANDO'
            output_rows.append(row)

        output_df = pd.DataFrame(output_rows, columns=template_cols)

        # Resumen para la tabla del frontend
        total_flota = len(fleet_df)
        estado_counts = fleet_df['ESTADO'].value_counts().to_dict() if total_flota else {}
        estado_pct = {k: round(v * 100 / total_flota, 2) for k, v in estado_counts.items()} if total_flota else {}
        table_data = [{"Estado": k, "Cantidad": v, "Porcentaje": estado_pct.get(k, 0)} for k, v in estado_counts.items()]

        # Excel en memoria para /download (usa openpyxl, no requiere xlsxwriter)
        excel_stream = io.BytesIO()
        with pd.ExcelWriter(excel_stream, engine="openpyxl") as writer:
            output_df.to_excel(writer, index=False, sheet_name=SHEET_NAME)
        excel_stream.seek(0)
        last_excel_stream = excel_stream

        # Nombre con fecha
        current_date = datetime.now().strftime("%d%m%Y")
        filename = f"vehicle_fleet_update_{current_date}.xlsx"

        return {
            "status": "success",
            "table_data": table_data,
            "filename": filename
        }

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


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
