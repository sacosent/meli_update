import pandas as pd

# Cargar archivos
fleet_df = pd.read_excel("fleet-moviles.xlsx")
disponibilidad_df = pd.read_excel("disponibilidad.xlsx")
modelo_df = pd.read_excel("Planilla-Modelo.xlsx")

# Filtrar columnas relevantes y normalizar
fleet_filtered = fleet_df[['Placa', 'Estado']].copy()
fleet_filtered['Placa'] = fleet_filtered['Placa'].astype(str).str.upper().str.strip()

# Placas en disponibilidad (ociosas)
disponibilidad_placas = disponibilidad_df['Veículo'].astype(str).str.upper().str.strip().unique()

# -------- Lógica de comparación --------

# Condición A: figura como "ATIVO - BIPANDO" pero también aparece en disponibilidad → debería ser "FROTA OCIOSA"
cond_a = fleet_filtered[
    (fleet_filtered['Estado'] == "ATIVO - BIPANDO") &
    (fleet_filtered['Placa'].isin(disponibilidad_placas))
].copy()
cond_a['Estado'] = "FROTA OCIOSA"

# Condición B: figura como "FROTA OCIOSA" pero NO aparece en disponibilidad → debería ser "ATIVO - BIPANDO"
cond_b = fleet_filtered[
    (fleet_filtered['Estado'] == "FROTA OCIOSA") &
    (~fleet_filtered['Placa'].isin(disponibilidad_placas))
].copy()
cond_b['Estado'] = "ATIVO - BIPANDO"

# Combinar ambos
result_df = pd.concat([cond_a, cond_b], ignore_index=True)

# Crear archivo con formato de modelo
output_df = modelo_df.iloc[0:0].copy()
output_df['Dominio'] = result_df['Placa'].values
output_df['Estado'] = result_df['Estado'].values

# Guardar resultado Excel
output_df.to_excel("Vehiculos_para_actualizar_estado.xlsx", index=False)

# -------- Resultados informativos --------
total_flota = fleet_filtered.shape[0]
errores_a = cond_a.shape[0]
errores_b = cond_b.shape[0]
total_errores = result_df.shape[0]

# Crear resumen en string
summary_lines = []
summary_lines.append("RESULTADOS DEL ANÁLISIS\n")
summary_lines.append(f"➡️  Tamaño total de la flota: {total_flota} vehículos\n")

summary_lines.append("📊 Distribución por estado en fleet-moviles:\n")
estado_counts = fleet_filtered['Estado'].value_counts()
for estado, count in estado_counts.items():
    pct = (count / total_flota) * 100
    summary_lines.append(f"   - {estado}: {count} vehículos ({pct:.2f}%)\n")

summary_lines.append("\n⚠️ Inconsistencias detectadas:\n")
summary_lines.append(f"🔄 Vehículos 'ATIVO - BIPANDO' que figuran en disponibilidad:\n")
summary_lines.append(f"   {errores_a} vehículos ({(errores_a / total_flota) * 100:.2f}%)\n")
summary_lines.append(f"🔄 Vehículos 'FROTA OCIOSA' que NO figuran en disponibilidad:\n")
summary_lines.append(f"   {errores_b} vehículos ({(errores_b / total_flota) * 100:.2f}%)\n")

summary_lines.append(f"\n🔄 Total de vehículos con estado a actualizar: {total_errores} vehículos ({(total_errores / total_flota) * 100:.2f}%)\n")


# Guardar como archivo de texto
with open("Resumen_analisis_vehiculos.txt", "w", encoding="utf-8") as f:
    f.writelines(summary_lines)

# Mostrar en consola también
print("".join(summary_lines))
print("📁 Archivo generado: Vehiculos_para_actualizar_estado.xlsx")