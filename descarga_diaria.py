from prefect import flow, task


@task
def descargar_datos_dia_anterior():
    from datetime import datetime, timedelta
    import requests
    import pandas as pd
    import os
    import json

    # 1. Calcular fechas
    antier = datetime.today() - timedelta(days=2)
    fecha_inicio = antier.strftime("%Y-%m-%dT00:00:00")
    fecha_fin = (antier + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")

    print(f"Fecha inicio: {fecha_inicio}")
    print(f"Fecha fin: {fecha_fin}")

    # 2. Construir URL
    url = f"https://carmob.com.co/ws2/wsIntegraContratante.ashx?Cod=2304&DI={fecha_inicio}&DF={fecha_fin}"
    print(f"Descargando datos desde: {url}")

    # 3. Hacer el request
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error HTTP {response.status_code} al descargar los datos.")
        return

    # 4. Decodificar el contenido de forma segura
    try:
        contenido = response.content.decode("utf-8-sig", errors="replace")
        data = json.loads(contenido)
    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
        print("Contenido parcial de la respuesta:")
        print(response.text[:1000])
        return
    except Exception as e:
        print(f"Error inesperado al procesar la respuesta: {e}")
        return

    print(f"Registros descargados: {len(data)}")

    # 5. Convertir a DataFrame
    df = pd.DataFrame(data)
    df["FechaConsulta"] = antier.strftime("%Y-%m-%d")

    # Filtrar por CodPark específico
    df = df[df["CodPark"].isin(["6064", "6065", "6066", "6067"])]

    # Procesar fechas y horas
    df["DateIn"] = pd.to_datetime(df["DateIn"], errors="coerce")
    df["fecha_in"] = df["DateIn"].dt.date
    df["hora_in"] = df["DateIn"].dt.time

    df["DataOut"] = pd.to_datetime(df["DataOut"], errors="coerce")
    df["fecha_out"] = df["DataOut"].dt.date
    df["hora_out"] = df["DataOut"].dt.time

    df["DatePayment"] = pd.to_datetime(df["DatePayment"], errors="coerce")
    df["fecha_payment"] = df["DatePayment"].dt.date

    # Completar valores faltantes
    df["fecha_in"] = df["fecha_in"].fillna(df["fecha_payment"])
    df["fecha_in"] = pd.to_datetime(df["fecha_in"])
    df["fecha_out"] = df["fecha_out"].fillna(df["fecha_in"])
    df["fecha_out"] = pd.to_datetime(df["fecha_out"])

    df["AmountPayment"] = pd.to_numeric(df["AmountPayment"], errors="coerce")

    # 6. Guardar CSV
    os.makedirs("output", exist_ok=True)
    nombre_archivo = f"registros_{antier.strftime('%Y-%m-%d')}.csv"
    ruta_archivo = os.path.join("output", nombre_archivo)

    # Guardar con codificación compatible con Excel
    df.to_csv(ruta_archivo, index=False, encoding="utf-8-sig")
    print(f"Datos guardados correctamente en: {ruta_archivo}")


@flow(name="Descargar_JSON_diario", log_prints=True)
def flujo_diario():
    descargar_datos_dia_anterior()


if __name__ == "__main__":
    flujo_diario()

