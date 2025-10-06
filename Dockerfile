# Imagen base oficial de Prefect 3
FROM prefecthq/prefect:3-latest

# Establecer el directorio de trabajo
WORKDIR /app

# Copiar archivos del proyecto al contenedor
COPY . /app

# Instalar dependencias si existen
RUN pip install --no-cache-dir -r requirements.txt || echo "No requirements.txt found"

# Variables de entorno (⚠️ reemplaza con tus valores reales)
ENV PREFECT_API_URL="https://api.prefect.cloud/api/accounts/8f2f7512-d919-4215-a278-04dfcb0d3ff3/workspaces/721f10f7-3356-452b-820f-3ba5d7548d67"
ENV PREFECT_API_KEY="pnu_ggnzNP2SxO8gF8JxeOfM7QLNxIn4Tt3dOCMo"
ENV PREFECT_LOGGING_LEVEL="INFO"

# Comando por defecto: iniciar worker
CMD ["prefect", "worker", "start", "-p", "descarga-managed"]