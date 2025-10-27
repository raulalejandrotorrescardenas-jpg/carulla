
from datetime import datetime, timedelta
import requests
import pandas as pd
import os



antier = datetime.today() - timedelta(days=2)
fecha_inicio = antier.strftime("%Y-%m-%dT00:00:00")
fecha_fin = (antier + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

# 🧠 Verificamos que las fechas estén correctas
print("📅 Fecha inicio:", fecha_inicio)
print("📅 Fecha fin:", fecha_fin)

# 🌐 2. Construir URL dinámica
url = f"https://carmob.com.co/ws2/wsIntegraContratante.ashx?Cod=2304&DI={fecha_inicio}&DF={fecha_fin}"

# 📥 3. Hacer el request
response = requests.get(url)
if response.status_code == 200:
    data = response.json()
print(f"✅ Registros descargados: {len(data)}")

# 📊 4. Convertir a DataFrame
df = pd.DataFrame(data)

# (Opcional) agregar la fecha de consulta
df["FechaConsulta"] = antier.strftime("%Y-%m-%d")

df=df[df["CodPark"].isin(["6064","6065","6066","6067"])]

df["DateIn"] = pd.to_datetime(df["DateIn"])

df["fecha_in"] = df["DateIn"].dt.date 
df["hora_in"] = df["DateIn"].dt.time 

df["DataOut"] = pd.to_datetime(df["DataOut"], format="mixed", errors="coerce")

df["fecha_out"] = df["DataOut"].dt.date 
df["hora_out"] = df["DataOut"].dt.time 

df["DatePayment"] = pd.to_datetime(df["DatePayment"], format="mixed", errors="coerce")

df["fecha_payment"] = df["DatePayment"].dt.date

df["fecha_in"] = df["fecha_in"].fillna(df["fecha_payment"])

df["fecha_in"] = pd.to_datetime(df["fecha_in"])

df["fecha_out"] = df["fecha_out"].fillna(df["fecha_in"])

df["fecha_out"] = pd.to_datetime(df["fecha_out"])

df["AmountPayment"]=pd.to_numeric(df["AmountPayment"], errors="coerce")

ruta_destino= "D:/DATA/One - rtorres/OneDrive - EQUIPARK SAS/carulla - Documentos"

fecha_in_g=antier.replace(hour=0, minute=0, second=0, microsecond=0)
fehca_fin_g=fecha_in_g + timedelta(days=1)



fecha_inicio_str = fecha_in_g.strftime("%Y-%m-%d")
fecha_fin_str = fehca_fin_g.strftime("%Y-%m-%d")
nombre_archivo = f"carulla_{fecha_inicio_str}_{fecha_fin_str}.csv"


ruta=os.path.join(ruta_destino,nombre_archivo)
# Guardar el CSV correctamente
df.to_csv(ruta, index=False, encoding="utf-8-sig")
print(f"✅ Datos guardados en: {ruta}")

