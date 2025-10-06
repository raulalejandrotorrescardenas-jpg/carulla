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

