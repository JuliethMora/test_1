import streamlit as st
import tempfile
import os
import subprocess
import shutil
from pathlib import Path
import glob
import pandas as pd
import numpy as np

# Verificar que la dependencia openpyxl está disponible en el entorno
try:
    import openpyxl  # noqa: F401
    _has_openpyxl = True
except Exception:
    _has_openpyxl = False

st.set_page_config(page_title="ETL AutoCAD", layout="wide")

st.title("🚀 Ejecución del ETL AutoCAD")
st.markdown("""
Esta aplicación ejecuta automáticamente el proceso ETL definido en **etlautocad.py**  
Sube el archivo Excel principal del proyecto, el archivo obligatorio para `INTERNO_PROYECTO` (se acepta cualquier nombre de archivo) y el archivo de items (`Items_CTO`, también puede llamarse como quieras). Luego presiona **Ejecutar ETL**.
""")

# ---- Entrada del usuario ----
uploaded_excel = st.file_uploader("📁 Sube el archivo Excel principal del proyecto", type=["xlsx", "xls"])

# Uploader obligatorio adicional (acepta cualquier nombre .xlsx)
uploaded_nterno = st.file_uploader("📁 Sube el archivo obligatorio para INTERNO_PROYECTO (cualquier archivo .xlsx)", type=["xlsx"], key="nterno")

# Uploader opcional para archivo Items_CTO (si no se sube, el script intentará detectarlo en el directorio)
uploaded_items = st.file_uploader("📁 (Opcional) Sube el archivo Items_CTO (Items_CTO_YYYY_XXXX.xlsx) — puede tener cualquier nombre", type=["xlsx"], key="items")

run_button = st.button("▶️ Ejecutar ETL")

# ---- Validaciones ----
if run_button:
    if not _has_openpyxl:
        st.error("Falta la dependencia opcional 'openpyxl'. Instálala con `pip install openpyxl` y vuelve a intentar.")
        st.stop()
    if not uploaded_excel:
        st.error("Por favor, sube el archivo Excel principal antes de ejecutar.")
        st.stop()

    # Validar uploader obligatorio (acepta cualquier nombre de archivo .xlsx)
    if not uploaded_nterno:
        st.error("El archivo obligatorio para INTERNO_PROYECTO no fue subido. Por favor súbelo antes de ejecutar.")
        st.stop()

    # Validar uploader de Items_CTO
    if not uploaded_items:
        st.error("El archivo de items (Items_CTO) no fue subido. Por favor súbelo antes de ejecutar.")
        st.stop()

    # Crear carpeta temporal
    tmp_dir = Path(tempfile.mkdtemp(prefix="etl_run_"))
    st.info(f"Directorio temporal creado: `{tmp_dir}`")

    # Guardar el Excel principal subido
    excel_path = tmp_dir / uploaded_excel.name
    with open(excel_path, "wb") as f:
        f.write(uploaded_excel.getbuffer())

    # Guardar el archivo obligatorio `INTERNO_PROYECTO.xlsx` en el tmp_dir (se acepta cualquier nombre subido)
    nterno_path = tmp_dir / "INTERNO_PROYECTO.xlsx"
    with open(nterno_path, "wb") as f:
        f.write(uploaded_nterno.getbuffer())

    # Guardar el archivo Items_CTO si fue subido (mantener nombre original para que etlautocad lo detecte)
    if uploaded_items:
        items_path = tmp_dir / uploaded_items.name
        with open(items_path, "wb") as f:
            f.write(uploaded_items.getbuffer())

        # Además crear una copia con nombre fijo para compatibilidad: Items_CTO.xlsx
        items_fixed = tmp_dir / "Items_CTO.xlsx"
        with open(items_fixed, "wb") as f:
            f.write(uploaded_items.getbuffer())

    # Copiar el script original
    original_script = Path("etlautocad.py")
    if not original_script.exists():
        st.error("No se encontró `etlautocad.py` en el mismo directorio que este script.")
        st.stop()

    # Copiarlo al directorio temporal
    tmp_script = tmp_dir / "etlautocad.py"
    shutil.copy(original_script, tmp_script)

    # Modificar el script para eliminar input() e insertar la ruta del Excel automáticamente
    content = tmp_script.read_text(encoding="utf-8")

    import re
    # Busca la línea con 'dataset = input(' y reemplaza con el path del Excel subido
    pattern = r'dataset\s*=\s*input\(.*\)\.strip\(\)'
    replacement = f'dataset = r"{excel_path.name}"'
    content = re.sub(pattern, replacement, content)

    tmp_script.write_text(content, encoding="utf-8")

    st.write("✅ Script preparado, iniciando ejecución...")

    # Ejecutar el ETL en el entorno temporal
    cmd = ["python", str(tmp_script.name)]
    log_placeholder = st.empty()
    logs = []

    with subprocess.Popen(
        cmd, cwd=tmp_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    ) as proc:
        for line in proc.stdout:
            logs.append(line)
            log_placeholder.text("".join(logs[-40:]))  # muestra últimas 40 líneas
        proc.wait()

   # ...existing code...
    st.success("Ejecución completada ✅")

    # Mostrar outputs generados
    all_outputs = list(tmp_dir.glob("*.xlsx")) + list(tmp_dir.glob("*.csv"))
    if not all_outputs:
        st.warning("No se detectaron archivos de salida. Verifica el log de ejecución.")
    else:
        # Filtrar archivos cuyo nombre contenga 'output' (case-insensitive)
        outputs_with_keyword = [f for f in all_outputs if 'output' in f.name.lower()]

        # Si hay archivos que contengan 'output', usarlos (hasta 3). Si no, usar hasta 3 cualquiera.
        if outputs_with_keyword:
            chosen = sorted(outputs_with_keyword, key=lambda p: p.name)[:3]
        else:
            st.warning("No se detectaron archivos que contengan 'output'. Mostrando hasta 3 archivos generados.")
            chosen = sorted(all_outputs, key=lambda p: p.name)[:3]

        st.subheader("📦 Archivos generados (hasta 3):")
        for f in chosen:
            if f.exists():
                with open(f, "rb") as file:
                    st.download_button(
                        label=f"Descargar {f.name}",
                        data=file.read(),
                        file_name=f.name,
                        mime="application/octet-stream",
                    )

        # Crear zip solo si hay archivos que contengan 'output' (hasta 3)
        if outputs_with_keyword:
            import zipfile
            chosen_for_zip = sorted(outputs_with_keyword, key=lambda p: p.name)[:3]
            zip_path = tmp_dir / "outputs_top3.zip"
            # Crear zip solo con los archivos seleccionados
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                for f in chosen_for_zip:
                    if f.is_file():
                        zf.write(f, arcname=f.name)

            with open(zip_path, "rb") as zf:
                st.download_button(
                    label="📥 Descargar los hasta 3 archivos 'output' (zip)",
                    data=zf.read(),
                    file_name=zip_path.name,
                    mime="application/zip",
                )
print(f"ETL execution completed")
