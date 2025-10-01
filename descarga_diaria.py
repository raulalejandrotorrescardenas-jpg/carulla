from prefect import flow, task



@task
def descargar_datos_dia_anterior():
    from datetime import datetime, timedelta
    import requests
    import pandas as pd
    import os

    antier = datetime.today() - timedelta(days=2)
    fecha_inicio = antier.strftime("%Y-%m-%dT00:00:00")
    fecha_fin = (antier + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

# Verificamos que las fechas estén correctas
    print(" Fecha inicio:", fecha_inicio)
    print(" Fecha fin:", fecha_fin)

#  2. Construir URL dinámica
    url = f"https://carmob.com.co/ws2/wsIntegraContratante.ashx?Cod=2304&DI={fecha_inicio}&DF={fecha_fin}"

#  3. Hacer el request
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(f" Registros descargados: {len(data)}")

    #  4. Convertir a DataFrame
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


    # 5. Guardar en CSV
    os.makedirs("output", exist_ok=True)
    nombre_archivo = f"registros_{antier.strftime('%Y-%m-%d')}.csv"
    ruta_archivo = os.path.join("output", nombre_archivo)

    # Guardar el CSV correctamente
    df.to_csv(ruta_archivo, index=False, encoding="utf-8-sig")
    print(f" Datos guardados en: {ruta_archivo}")



@flow(name="Descargar JSON diario", log_prints=True)
def flujo_diario():
    descargar_datos_dia_anterior()

if __name__ == "__main__":
    # para probar localmente (crea el CSV en ./output)
    flujo_diario()
