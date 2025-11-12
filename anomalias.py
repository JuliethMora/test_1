import streamlit as st
import tempfile
from pathlib import Path
import pandas as pd
import numpy as np
import shutil
import zipfile
import os

# --------------------------
# Configuración de Streamlit
# --------------------------
st.set_page_config(page_title="ETL AutoCAD Integrado", layout="wide")
st.title("🚀 ETL AutoCAD Integrado")
st.markdown("""
Esta aplicación ejecuta automáticamente el proceso ETL sin depender de archivos externos.  
Sube el archivo Excel principal del proyecto, el archivo obligatorio para `INTERNO_PROYECTO` y el archivo `Items_CTO` (opcional). Luego presiona **Ejecutar ETL**.
""")

# --------------------------
# Verificación dependencias
# --------------------------
try:
    import openpyxl  # noqa: F401
    _has_openpyxl = True
except Exception:
    _has_openpyxl = False

# Patch seguro para pandas.read_excel
_orig_read_excel = pd.read_excel
def _safe_read_excel(*args, **kwargs):
    try:
        return _orig_read_excel(*args, **kwargs)
    except ImportError as e:
        msg = str(e)
        if 'openpyxl' in msg or "Missing optional dependency 'openpyxl'" in msg:
            st.error("❌ Error al leer archivo: falta la dependencia 'openpyxl'. Instálala con `pip install openpyxl`.")
            st.stop()
        raise
pd.read_excel = _safe_read_excel

# --------------------------
# Subida de archivos
# --------------------------
uploaded_excel = st.file_uploader("📁 Sube el archivo Excel principal del proyecto", type=["xlsx", "xls"])
uploaded_nterno = st.file_uploader("📁 Sube el archivo obligatorio para INTERNO_PROYECTO", type=["xlsx"], key="nterno")
uploaded_items = st.file_uploader("📁 (Opcional) Sube el archivo Items_CTO", type=["xlsx"], key="items")

run_button = st.button("▶️ Ejecutar ETL")

# --------------------------
# Ejecución del ETL integrado
# --------------------------
if run_button:
    if not _has_openpyxl:
        st.error("Falta la dependencia 'openpyxl'. Instálala con `pip install openpyxl` y vuelve a intentar.")
        st.stop()
    if not uploaded_excel or not uploaded_nterno:
        st.error("Faltan archivos obligatorios. Por favor súbelos antes de ejecutar.")
        st.stop()

    # Crear carpeta temporal
    tmp_dir = Path(tempfile.mkdtemp(prefix="etl_run_"))
    st.info(f"Directorio temporal creado: `{tmp_dir}`")

    # Guardar archivos subidos
    excel_path = tmp_dir / uploaded_excel.name
    with open(excel_path, "wb") as f:
        f.write(uploaded_excel.getbuffer())

    nterno_path = tmp_dir / "INTERNO_PROYECTO.xlsx"
    with open(nterno_path, "wb") as f:
        f.write(uploaded_nterno.getbuffer())

    if uploaded_items:
        items_path = tmp_dir / uploaded_items.name
        with open(items_path, "wb") as f:
            f.write(uploaded_items.getbuffer())
        items_fixed = tmp_dir / "Items_CTO.xlsx"
        shutil.copy(items_path, items_fixed)

    # --------------------------
    # Simulación de ETL
    # --------------------------
    log_placeholder = st.empty()
    logs = []

    def log(msg):
        logs.append(msg + "\n")
        log_placeholder.text("".join(logs[-40:]))

    log("📌 Leyendo Excel principal...")
    try:
        df_project = pd.read_excel(excel_path)
        log(f"✅ Excel principal cargado: {excel_path.name}, {df_project.shape[0]} filas")
    except Exception as e:
        st.error(f"Error leyendo Excel principal: {e}")
        st.stop()

    log("📌 Leyendo INTERNO_PROYECTO...")
    try:
        df_nterno = pd.read_excel(nterno_path)
        log(f"✅ INTERNO_PROYECTO cargado: {nterno_path.name}, {df_nterno.shape[0]} filas")
    except Exception as e:
        st.error(f"Error leyendo INTERNO_PROYECTO: {e}")
        st.stop()

    if uploaded_items:
        log("📌 Leyendo Items_CTO...")
        try:
            df_items = pd.read_excel(items_fixed)
            log(f"✅ Items_CTO cargado: {items_fixed.name}, {df_items.shape[0]} filas")
        except Exception as e:
            st.error(f"Error leyendo Items_CTO: {e}")
            st.stop()
    else:
        df_items = pd.DataFrame()
        log("⚠️ No se subió archivo Items_CTO. Se procede sin él.")

    # --------------------------
    # ETL básico de ejemplo
    # --------------------------
    log("⚙️ Procesando datos...")
    try:
        # Ejemplo: combinar datos del proyecto con INTERNO_PROYECTO
        df_merged = pd.merge(df_project, df_nterno, how='left', on=df_project.columns[0])
        log(f"✅ Datos combinados, {df_merged.shape[0]} filas")

        # Ejemplo: agregar columna calculada
        df_merged["Total"] = df_merged.select_dtypes(include=np.number).sum(axis=1)
        log("✅ Columna 'Total' agregada")

        # Guardar resultado
        output_path = tmp_dir / "output_proyecto.xlsx"
        df_merged.to_excel(output_path, index=False)
        log(f"📦 Archivo de salida generado: {output_path.name}")
    except Exception as e:
        st.error(f"Error en procesamiento ETL: {e}")
        st.stop()

    # --------------------------
    # Mostrar archivos generados
    # --------------------------
    all_outputs = list(tmp_dir.glob("*.xlsx")) + list(tmp_dir.glob("*.csv"))
    st.subheader("📦 Archivos generados:")
    for f in all_outputs:
        with open(f, "rb") as file:
            st.download_button(
                label=f"Descargar {f.name}",
                data=file.read(),
                file_name=f.name,
                mime="application/octet-stream",
            )

    # Crear ZIP con hasta 3 archivos
    zip_path = tmp_dir / "outputs_top3.zip"
    with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(all_outputs, key=lambda x: x.name)[:3]:
            zf.write(f, arcname=f.name)
    with open(zip_path, "rb") as zf:
        st.download_button(
            label="📥 Descargar hasta 3 archivos (zip)",
            data=zf.read(),
            file_name=zip_path.name,
            mime="application/zip",
        )

    st.success("✅ ETL completado")
