from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import tempfile
import os

app = FastAPI()

@app.post("/procesar")
async def procesar_excel(
    fleet_file: UploadFile = File(...),
    disponibilidad_file: UploadFile = File(...),
):
    # Crear archivos temporales
    with tempfile.TemporaryDirectory() as tmpdir:
        fleet_path = os.path.join(tmpdir, "fleet.xlsx")
        disp_path = os.path.join(tmpdir, "disponibilidad.xlsx")
        output_path = os.path.join(tmpdir, "Vehiculos_para_actualizar_estado.xlsx")

        with open(fleet_path, "wb") as f:
            f.write(await fleet_file.read())
        with open(disp_path, "wb") as f:
            f.write(await disponibilidad_file.read())

        # Cargar data
        fleet_df = pd.read_excel(fleet_path)
        disponibilidad_df = pd.read_excel(disp_path)

        fleet_filtered = fleet_df[['Placa', 'Estado']].copy()
        fleet_filtered['Placa'] = fleet_filtered['Placa'].astype(str).str.upper().str.strip()
        disponibilidad_placas = disponibilidad_df['Veículo'].astype(str).str.upper().str.strip().unique()

        cond_a = fleet_filtered[
            (fleet_filtered['Estado'] == "ATIVO - BIPANDO") &
            (fleet_filtered['Placa'].isin(disponibilidad_placas))
        ].copy()
        cond_a['Estado'] = "FROTA OCIOSA"

        cond_b = fleet_filtered[
            (fleet_filtered['Estado'] == "FROTA OCIOSA") &
            (~fleet_filtered['Placa'].isin(disponibilidad_placas))
        ].copy()
        cond_b['Estado'] = "ATIVO - BIPANDO"

        result_df = pd.concat([cond_a, cond_b], ignore_index=True)

        # Crear Excel final
        output_df = pd.DataFrame(columns=["Dominio", "Estado"])
        output_df['Dominio'] = result_df['Placa'].values
        output_df['Estado'] = result_df['Estado'].values
        output_df.to_excel(output_path, index=False)

        return FileResponse(output_path, filename="Vehiculos_para_actualizar_estado.xlsx")
