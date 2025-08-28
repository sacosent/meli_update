from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import pandas as pd
import io
from datetime import datetime
from uuid import uuid4
from typing import Dict, Optional, List

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# --- Config ---
TEMPLATE_PATH = "./Planilla-Modelo.xlsx"   # Debe existir en la raíz del proyecto
SHEET_NAME = "Worksheet"                   # Si no existe, se usa la primera hoja
download_store: Dict[str, io.BytesIO] = {}  # Token -> stream en memoria


# ---------- Utilidades ----------
def _normalize_key(s: str) -> str:
    return (
        str(s)
        .strip()
        .upper()
        .replace("Á", "A").replace("É", "E").replace("Í", "I")
        .replace("Ó", "O").replace("Ú", "U").replace("Ç", "C")
    )

def _pick_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Devuelve el nombre real de columna del DF matcheando candidatos (case/acento/espacios insensible)."""
    norm_map = {_normalize_key(c): c for c in df.columns}
    for cand in candidates:
        k = _normalize_key(cand)
        if k in norm_map:
            return norm_map[k]
    return None

def _load_template_headers(template_path: str, sheet_name: Optional[str]) -> List[str]:
    """Lee solo encabezados del template, intentando la hoja pedida y si no, la primera."""
    xl = pd.ExcelFile(template_path)
    target = sheet_name if (sheet_name and sheet_name in xl.sheet_names) else xl.sheet_names[0]
    df_headers = pd.read_excel(template_path, sheet_name=target, nrows=0)
    return list(df_headers.columns)

def _find_template_col(template_cols: List[str], *names: str) -> Optional[str]:
    """Encuentra el nombre REAL de columna del template por alias (case-insensible, acentos tolerantes)."""
    m = {_normalize_key(c): c for c in template_cols}
    for n in names:
        key = _normalize_key(n)
        if key in m:
            return m[key]
    return None

def _most_frequent(series: pd.Series) -> Optional[str]:
    """Devuelve la moda (valor más frecuente) no-nulo en formato str/upper/strip."""
    if series is None:
        return None
    s = series.dropna().astype(str).str.upper().str.strip()
    if s.empty:
        return None
    return s.value_counts().idxmax()


# ---------- Rutas ----------
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
    - Lee fleet-moviles y disponibilidad
    - Reglas de ESTADO (existente) + NUEVAS reglas:
      * SVC (Base): si fleet != disponibilidad, actualizar
      * MLP (Centro de Custos): si fleet != disponibilidad, actualizar
    - Una fila por patente combinando cambios de Estado/SVC/MLP
    - Excel respeta estructura del template y se guarda en memoria con token
    """
    try:
        fleet_bytes = await fleet_file.read()
        disp_bytes  = await disponibilidad_file.read()

        fleet_df = pd.read_excel(io.BytesIO(fleet_bytes))
        disp_df  = pd.read_excel(io.BytesIO(disp_bytes))

        # Normalizar encabezados (conservar nombres reales, pero usaremos búsqueda tolerante)
        fleet_df.columns = fleet_df.columns.str.strip()
        disp_df.columns  = disp_df.columns.str.strip()

        # Identificadores de vehículo
        placa_col   = _pick_col(fleet_df, "PLACA", "DOMINIO", "PLATE", "PATENTE")
        veic_col    = _pick_col(disp_df,  "VEÍCULO", "VEICULO", "VEHICULO", "DOMINIO", "PLACA", "PATENTE")

        # Estado
        estado_col  = _pick_col(fleet_df, "ESTADO", "STATUS")

        # SVC (Base) y MLP (Centro de Custos) en ambas planillas
        svc_fleet_col = _pick_col(fleet_df, "BASE")  # Col C en fleet
        mlp_fleet_col = _pick_col(fleet_df, "CENTRO DE CUSTOS", "CENTRO DE CUSTO")  # Col T en fleet

        svc_disp_col = _pick_col(disp_df, "BASE")  # Col E en disponibilidad
        mlp_disp_col = _pick_col(disp_df, "CENTRO DE CUSTOS", "CENTRO DE CUSTO")  # Col I en disponibilidad

        # Validaciones mínimas
        if not placa_col:
            return JSONResponse({"error": "Não encontrei a coluna de PLACA/DOMINIO na planilha de frota."}, status_code=400)
        if not veic_col:
            return JSONResponse({"error": "Não encontrei a coluna de VEÍCULO/VEICULO/PLACA na planilha de disponibilidade."}, status_code=400)

        # Normalizaciones comunes
        fleet_df[placa_col] = fleet_df[placa_col].astype(str).str.upper().str.strip()
        disp_df[veic_col]   = disp_df[veic_col].astype(str).str.upper().str.strip()

        if estado_col and estado_col in fleet_df.columns:
            fleet_df[estado_col] = fleet_df[estado_col].astype(str).str.upper().str.strip()

        if svc_fleet_col and svc_fleet_col in fleet_df.columns:
            fleet_df[svc_fleet_col] = fleet_df[svc_fleet_col].astype(str).str.upper().str.strip()
        if mlp_fleet_col and mlp_fleet_col in fleet_df.columns:
            fleet_df[mlp_fleet_col] = fleet_df[mlp_fleet_col].astype(str).str.upper().str.strip()

        if svc_disp_col and svc_disp_col in disp_df.columns:
            disp_df[svc_disp_col] = disp_df[svc_disp_col].astype(str).str.upper().str.strip()
        if mlp_disp_col and mlp_disp_col in disp_df.columns:
            disp_df[mlp_disp_col] = disp_df[mlp_disp_col].astype(str).str.upper().str.strip()

        # --- Reglas de ESTADO (existentes) ---
        activos = set()
        ociosos = set()
        if estado_col and estado_col in fleet_df.columns:
            activos = set(fleet_df[fleet_df[estado_col] == 'ATIVO - BIPANDO'][placa_col])
            ociosos = set(fleet_df[fleet_df[estado_col] == 'FROTA OCIOSA'][placa_col])

        # Este set se usaba para inferir "debería ser ocioso/activo" (mantener)
        disp_set = set(disp_df[veic_col].dropna())

        activos_deberian_ser_ociosos = activos & disp_set       # están ativos pero deberían ser ociosos
        ociosos_deberian_ser_activos = ociosos - disp_set       # están ociosos pero deberían ser ativos

        # --- Reglas NUEVAS: SVC/MLP objetivo desde disponibilidad (moda por patente) ---
        svc_target_map = {}
        mlp_target_map = {}
        if svc_disp_col and svc_disp_col in disp_df.columns:
            svc_target_map = disp_df.groupby(veic_col)[svc_disp_col].apply(_most_frequent).to_dict()
        if mlp_disp_col and mlp_disp_col in disp_df.columns:
            mlp_target_map = disp_df.groupby(veic_col)[mlp_disp_col].apply(_most_frequent).to_dict()

        # Valores actuales en flota
        svc_current_map = {}
        mlp_current_map = {}
        if svc_fleet_col and svc_fleet_col in fleet_df.columns:
            svc_current_map = fleet_df.set_index(placa_col)[svc_fleet_col].to_dict()
        if mlp_fleet_col and mlp_fleet_col in fleet_df.columns:
            mlp_current_map = fleet_df.set_index(placa_col)[mlp_fleet_col].to_dict()

        # --- Plantilla (mismas columnas/orden) ---
        template_cols = _load_template_headers(TEMPLATE_PATH, SHEET_NAME)

        dominio_tpl = _find_template_col(template_cols, "Dominio", "Placa", "Patente", "DOMINIO")
        estado_tpl  = _find_template_col(template_cols, "Estado", "STATUS", "ESTADO")
        base_tpl    = _find_template_col(template_cols, "Base")  # SVC
        mlp_tpl     = _find_template_col(template_cols, "Centro de Custos", "Centro de Custo")

        if not dominio_tpl or not estado_tpl:
            return JSONResponse({"error": "El template debe incluir 'Dominio' y 'Estado'."}, status_code=500)
        # base_tpl / mlp_tpl pueden ser None; si no existen en el template, no se llenan.

        # Construcción: una fila por patente con todos los cambios
        changes_summary = {"svc_changes": 0, "mlp_changes": 0, "estado_changes": 0, "both_changes": 0, "total_rows": 0}
        rows_by_plate: Dict[str, dict] = {}

        def ensure_row(plate: str) -> dict:
            if plate not in rows_by_plate:
                rows_by_plate[plate] = {col: "" for col in template_cols}
                rows_by_plate[plate][dominio_tpl] = plate
            return rows_by_plate[plate]

        # Cambios de ESTADO (regla original)
        for plate in sorted(activos_deberian_ser_ociosos):
            row = ensure_row(plate)
            row[estado_tpl] = 'FROTA OCIOSA'
            changes_summary["estado_changes"] += 1
        for plate in sorted(ociosos_deberian_ser_activos):
            row = ensure_row(plate)
            row[estado_tpl] = 'ATIVO - BIPANDO'
            changes_summary["estado_changes"] += 1

        # Cambios de SVC/MLP (comparación directa fleet vs disponibilidad)
        for plate in fleet_df[placa_col].unique():
            svc_tgt = svc_target_map.get(plate)
            mlp_tgt = mlp_target_map.get(plate)
            svc_cur = svc_current_map.get(plate)
            mlp_cur = mlp_current_map.get(plate)

            svc_changed = False
            mlp_changed = False

            if base_tpl and svc_tgt and svc_tgt != (svc_cur or ""):
                row = ensure_row(plate)
                row[base_tpl] = svc_tgt
                svc_changed = True

            if mlp_tpl and mlp_tgt and mlp_tgt != (mlp_cur or ""):
                row = ensure_row(plate)
                row[mlp_tpl] = mlp_tgt
                mlp_changed = True

            if svc_changed and mlp_changed:
                changes_summary["both_changes"] += 1
            if svc_changed:
                changes_summary["svc_changes"] += 1
            if mlp_changed:
                changes_summary["mlp_changes"] += 1

        # DataFrame final
        output_rows = list(rows_by_plate.values())
        output_df = pd.DataFrame(output_rows, columns=template_cols)
        changes_summary["total_rows"] = len(output_df)

        # Resumen por Estado (para tabla del frontend)
        total_flota = len(fleet_df)
        estado_counts = {}
        estado_pct = {}
        if estado_col and total_flota:
            estado_counts = fleet_df[estado_col].value_counts().to_dict()
            estado_pct = {k: round(v * 100 / total_flota, 2) for k, v in estado_counts.items()}
        table_data = [{"Estado": k, "Cantidad": v, "Porcentaje": estado_pct.get(k, 0)} for k, v in estado_counts.items()]

        # Excel en memoria (openpyxl)
        excel_stream = io.BytesIO()
        with pd.ExcelWriter(excel_stream, engine="openpyxl") as writer:
            sheet_name = SHEET_NAME if SHEET_NAME else "Worksheet"
            output_df.to_excel(writer, index=False, sheet_name=sheet_name)
        excel_stream.seek(0)

        # Token para descarga concurrente
        token = str(uuid4())
        download_store[token] = excel_stream

        current_date = datetime.now().strftime("%d%m%Y")
        filename = f"vehicle_fleet_update_{current_date}.xlsx"

        return {
            "status": "success",
            "table_data": table_data,
            "filename": filename,
            "token": token,
            "changes_summary": changes_summary
        }

    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/download/{token}")
async def download_excel(token: str):
    stream = download_store.pop(token, None)
    if stream is None:
        return JSONResponse({"error": "Arquivo não disponível para download."}, status_code=400)

    current_date = datetime.now().strftime("%d%m%Y")
    filename = f"vehicle_fleet_update_{current_date}.xlsx"

    stream.seek(0)
    return StreamingResponse(
        stream,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
