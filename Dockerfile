# Dockerfile

# 1. IMAGEN BASE: Usamos la misma imagen de Prefect que ya usabas.
FROM prefecthq/prefect:3-python3.12

# ==========================================================
# 🚨 CORRECCIÓN CLAVE: Configuración de la Localización 🚨
# Esto fuerza al sistema operativo base a usar UTF-8, 
# resolviendo el error 'charmap' que ocurre durante el inicio
# de Python o al leer metadatos de archivos.
# ==========================================================

# Instala el paquete de localización
RUN apt-get update && apt-get install -y locales && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Configura explícitamente el sistema para usar UTF-8
ENV LANG C.UTF-8
ENV LC_ALL C.UTF-8

# ==========================================================
# CÓDIGO DE CONSTRUCCIÓN DEL PROYECTO
# ==========================================================

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia e instala las dependencias primero
# Esto usa el archivo requirements.txt que contiene:
# pandas, requests, openpyxl, etc.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de tu proyecto 
# (descarga_diaria.py, prefect.yaml, y cualquier otro script necesario)
COPY . .
