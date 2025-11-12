import streamlit as st
import tempfile
import os
import subprocess
import shutil
from pathlib import Path
import re
import pandas as pd
import numpy as np
import sys
import zipfile
import pyodbc  # 👈 agregado para trabajar con Access

# --------------------------
# Configuración general
# --------------------------
st.set_page_config(page_title="ETL AutoCAD + Access", layout="wide")

st.title("🚀 Ejecución del ETL AutoCAD")
st.markdown("""
Esta aplicación ejecuta automáticamente el proceso ETL definido en **etlautocad.py**  
Sube el archivo Excel principal del proyecto, el archivo obligatorio para `INTERNO_PROYECTO` y el archivo de items (`Items_CTO`).  
Además, puedes subir una base de datos **Access (.accdb / .mdb)** y verificar su conexión.
""")

# --------------------------
# Verificación dependencias
# --------------------------
try:
    import openpyxl  # noqa: F401
    _has_openpyxl = True
except Exception:
    _has_openpyxl = False

# --------------------------
# Subida de archivos
# --------------------------
uploaded_excel = st.file_uploader("📁 Sube el archivo Excel principal del proyecto", type=["xlsx", "xls"])
uploaded_nterno = st.file_uploader("📁 Sube el archivo obligatorio para INTERNO_PROYECTO", type=["xlsx"], key="nterno")
uploaded_items = st.file_uploader("📁 (Opcional) Sube el archivo Items_CTO", type=["xlsx"], key="items")

run_button = st.button("▶️ Ejecutar ETL")

# --------------------------
# Ejecución del ETL
# --------------------------
if run_button:
    # Validaciones básicas
    if not _has_openpyxl:
        st.error("Falta la dependencia 'openpyxl'. Instálala con `pip install openpyxl` y vuelve a intentar.")
        st.stop()
    if not uploaded_excel or not uploaded_nterno:
        st.error("Faltan archivos obligatorios. Por favor súbelos antes de ejecutar.")
        st.stop()
    
    # Crear carpeta temporal
    tmp_dir = Path(tempfile.mkdtemp(prefix="etl_run_"))
    st.info(f"📂 Directorio temporal creado: `{tmp_dir}`")
    
    # Guardar el Excel principal subido
    excel_path = tmp_dir / uploaded_excel.name
    with open(excel_path, "wb") as f:
        f.write(uploaded_excel.getbuffer())
    
    # Guardar el archivo obligatorio INTERNO_PROYECTO.xlsx
    nterno_path = tmp_dir / "INTERNO_PROYECTO.xlsx"
    with open(nterno_path, "wb") as f:
        f.write(uploaded_nterno.getbuffer())
    
    # Guardar archivo opcional Items_CTO
    if uploaded_items:
        items_path = tmp_dir / uploaded_items.name
        with open(items_path, "wb") as f:
            f.write(uploaded_items.getbuffer())

        # Crear una copia con nombre fijo
        items_fixed = tmp_dir / "Items_CTO.xlsx"
        shutil.copy(items_path, items_fixed)
    
    # Verificar que exista el script etlautocad.py
    original_script = Path("etlautocad.py")
    if not original_script.exists():
        st.error("❌ No se encontró `etlautocad.py` en el mismo directorio que este script.")
        st.stop()
    
    # Copiar etlautocad.py a la carpeta temporal
    tmp_script = tmp_dir / "etlautocad.py"
    shutil.copy(original_script, tmp_script)
    
    # Reemplazar input() con el nombre del archivo subido
    content = tmp_script.read_text(encoding="utf-8")
    pattern = r'dataset\s*=\s*input\(.*\)\.strip\(\)'
    replacement = f'dataset = r"{excel_path.name}"'
    content = re.sub(pattern, replacement, content)
    tmp_script.write_text(content, encoding="utf-8")
    
    st.write("✅ Script preparado. Iniciando ejecución del ETL...")
    
    # 🔧 Ejecutar usando el mismo intérprete de Python
    cmd = [sys.executable, str(tmp_script.name)]
    log_placeholder = st.empty()
    logs = []

    with subprocess.Popen(
        cmd, cwd=tmp_dir, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1
    ) as proc:
        for line in proc.stdout:
            logs.append(line)
            log_placeholder.text("".join(logs[-40:]))
        proc.wait()

    st.success("Ejecución completada ✅")

    # --------------------------
    # Mostrar resultados
    # --------------------------
    all_outputs = list(tmp_dir.glob("*.xlsx")) + list(tmp_dir.glob("*.csv"))
    if not all_outputs:
        st.warning("⚠️ No se detectaron archivos de salida. Verifica el log de ejecución.")
    else:
        outputs_with_keyword = [f for f in all_outputs if 'output' in f.name.lower()]
        chosen = sorted(outputs_with_keyword, key=lambda p: p.name)[:3] if outputs_with_keyword else sorted(all_outputs, key=lambda p: p.name)[:3]
        
        st.subheader("📦 Archivos generados (hasta 3):")
        for f in chosen:
            if f.exists():
                with open(f, "rb") as file:
                    st.download_button(
                        label=f"⬇️ Descargar {f.name}",
                        data=file.read(),
                        file_name=f.name,
                        mime="application/octet-stream",
                    )

        if outputs_with_keyword:
            zip_path = tmp_dir / "outputs_top3.zip"
            with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                for f in chosen:
                    if f.is_file():
                        zf.write(f, arcname=f.name)
            with open(zip_path, "rb") as zf:
                st.download_button(
                    label="📥 Descargar ZIP con los archivos 'output'",
                    data=zf.read(),
                    file_name=zip_path.name,
                    mime="application/zip",
                )

# ===============================================================
# 🔹 NUEVA SECCIÓN: Cargar y verificar base de datos Access
# ===============================================================
st.markdown("---")
st.header("🗄️ Cargar Base de Datos Access (.accdb / .mdb)")

uploaded_access = st.file_uploader("📁 Sube tu base de datos Access", type=["accdb", "mdb"], key="access")

if uploaded_access:
    tmp_dir_access = Path(tempfile.mkdtemp(prefix="access_db_"))
    db_path = tmp_dir_access / uploaded_access.name
    with open(db_path, "wb") as f:
        f.write(uploaded_access.getbuffer())

    st.success(f"✅ Base de datos guardada temporalmente en: `{db_path}`")

    if st.button("🔗 Conectar y listar tablas de Access"):
        try:
            conn_str = (
                r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
                f"DBQ={db_path};"
            )
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()

            tables = [row.table_name for row in cursor.tables(tableType='TABLE')]
            if tables:
                st.success("✅ Conexión exitosa. Tablas encontradas:")
                st.write(tables)
            else:
                st.warning("La base de datos no contiene tablas visibles.")

            conn.close()
        except Exception as e:
            st.error(f"❌ Error al conectar con Access: {e}")
else:
    st.info("📂 Sube un archivo `.accdb` o `.mdb` para verificar conexión.")
