# -*- coding: utf-8 -*-
"""Flight Prices.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/11z2hcyTJzN2aVyx-W3T_G4rYoo5oSx5G

Análisis de Rango de Precios de Vuelos
"""

#Importamos todas las librerias que vamos a necesitar para poder trabajar con la API
import requests
import json
import pandas as pd
import time
from itertools import product
import os
from configparser import ConfigParser

from pathlib import Path

config = ConfigParser()

#Configuramos nuestro archivo congif.ini
config_dir = "config.ini"

config.read(config_dir)

"""En el siguiente código vamos a conectarnos con las credenciales que obtuvimos de la página de Amadeus (Api_Key y Api_Secret) que tenemos almacenadas en nuestro archivo config.ini, para hacer un post a la página que para nos entregue un Token para poder hacer la solicitud correspondiente a la API."""

import requests
config = ConfigParser()
config.read("config/config.ini")
config_dir = "config.ini"
config.read(config_dir)
url = "https://test.api.amadeus.com/v1/security/oauth2/token"
headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}
data = {
    "grant_type": "client_credentials",
    "client_id": config["api_amadeus"]["api_key"],
    "client_secret": config["api_amadeus"]["api_secret"],
}
response = requests.post(url, headers=headers, data=data)
if response.status_code == 200:
    token_info = response.json()
    access_token = token_info.get("access_token")
    print("Token:", access_token)
else:
    print("Error:", response.status_code, response.text)

# Definimos en variables nuestra url base y el endpoint a donde iremos a buscar la información
url_base = "https://test.api.amadeus.com/v1"
endpoint = "analytics/itinerary-price-metrics"

#En esta variable definimos tres listas con los distintos parametros a los cuales les vamos a pedir información de precios de los vuelos, para que vaya iterando sobre las distintas combinaciones.
origins = ["LIS","FCO","DUB","BUD","HEL","PMI","BER","SVQ"]
destinations = ['CDG','AMS',"BCN","VIE","BRU","LHR","VLC","NAP"]
dates= ['2023-12-01','2023-12-03',"2023-12-05","2023-12-09","2023-12-11","2023-12-15","2023-12-19"]

#Creamos una lista en donde se van a ir incoporando los resultados de nuestras solicitudes.
results = []

#Definimos el headers, en donde almacenaremos nuestro token para poder hacer las solicitudes a la API.
headers = {
    "Authorization": f"Bearer {token_info['access_token']}",
   }

# Generamos todas las combinaciones de los parámetros
combinaciones = product(origins, destinations, dates)

# Iteramos sobre todas las combinaciones de los parámetros
for origin,destination,date in combinaciones:
            # Definimos los parámetros en un diccionario
            parametros = {
                "originIataCode": origin,
                "destinationIataCode": destination,
                "departureDate": date
            }
            endpoint_url = f"{url_base}/{endpoint}"
            resp = requests.get(endpoint_url,headers=headers,
                params = parametros)

            if resp.status_code == 200:
              # Analizar la respuesta JSON y agregarla a la lista de resultados
                data = resp.json()
                info= data["data"]
                results.append(info)
            else:
                print(f"Error en la solicitud para Origen: {origin}, Destino: {destination}, Fecha: {date}")
                print(f"Código de error: {resp.status_code}")

#Aca podemos ver el resultado con una lista de diccionarios con lo que obtuvimos de iterar distintos parámetros en cada petición.
results

"""Con este próximo pasos lo que buscamos es convertir esa lista en algo mas legible y ordenado, unformato que nos resulte mas amigable para trabajar y leer los datos."""

# Desenrollar la lista anidada
data_unrolled = [item for sublist in results for item in sublist]

# Convertir la lista en un DataFrame normalizado
df = pd.json_normalize(data_unrolled)

# Visualizar el DataFrame resultante
print(df)

"""Con el código que vemos abajo, lo que vamos a hacer es poder sacar los distintos valores que estan almacenados en una misma columna, y crear distintas columnas con cada uno de los percentiles. De esta forma, tenemos la información en un formato mucho mas legible y que nos sirva para nuestro trabajo."""

# Normalizar la columna 'priceMetrics'
df = df.explode('priceMetrics')

# Expandir los datos en las columnas separadas
df = pd.concat(
    [df.drop(['priceMetrics'], axis=1), df['priceMetrics'].apply(pd.Series)], axis=1
    )

# Eliminar filas duplicadas
df = df.drop_duplicates()

# Utilizar pivot_table para crear columnas separadas para cada valor de 'quartileRanking'
df = df.pivot_table(
    index=[
        'type',
        'origin.iataCode',
        'destination.iataCode',
        'departureDate',
        'transportType',
        'currencyCode',
        'oneWay'
        ],
        columns='quartileRanking',
        values='amount',
        aggfunc='first'
  )
# Reiniciar el índice
df = df.reset_index()

# Cambiar los nombres de las columnas (opcional)
df.columns.name = None

# Visualizar el DataFrame resultante
print(df)

#Pasamos los datos a un dataframe en donde cada fila corresponde a un tramo distinto y las variaciones de precio que puede tener.
result= pd.DataFrame(df)
result

#Eliminamos las columnas type y transportType ya que no tienen ningun valor para nosotros.
result_flights= result.drop(["type","transportType"], axis=1)
result_flights

#Acomodamos en orden las columnas y renombramos algunas columnas
flights =result_flights[
    [
        'origin.iataCode',
        'destination.iataCode',
        'departureDate',
        'currencyCode',
        'oneWay',
        'MINIMUM',
        'FIRST',
        'MEDIUM',
        'THIRD',
        'MAXIMUM' ]
    ]
flights= flights.rename(
    columns={'origin.iataCode': 'origin', 'destination.iataCode': 'destination'}
    )

flights

"""Conexión a Redshift y Creación de la Tabla"""

# Commented out IPython magic to ensure Python compatibility.
# %pip install "redshift_connector[full]" sqlalchemy-redshift
import sqlalchemy as sa

#Creamos la función
def build_conn_string(config_path, config_section):
    """
    Construye la cadena de conexión a la base de datos
    a partir de un archivo de configuración.
    """

    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración de PostgreSQL
    config = parser[config_section]
    host = config['host']
    port = config['port']
    dbname = config['dbname']
    username = config['username']
    pwd = config['pwd']

    # Construye la cadena de conexión
    conn_string = f'postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require'

    return conn_string

#Creamos la conexión con la base de datos
def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine

#Utilizamos la función que creamos anteriormente para leer dentro de nuestro archivo config.ini las credenciales correspondientes a Redshift
conn_str = build_conn_string('config.ini', 'redshift')
conn, engine = connect_to_db(conn_str)

#Creamos la tabla en Redshift, donde más adelante pasaremos los datos que obtenemos desde nuestra API. Creamos una tabla de staggin de almacenamiento temporal y una tabla de dimension para datos únicos.
schema = "lucasforesto89_coderhuose"

conn.execute(
    f"""
        DROP TABLE IF EXISTS {schema}.stg;
        CREATE TABLE {schema}.stg (
            origin VARCHAR(20) distkey,
            destination VARCHAR(20),
            departureDate DATE,
            currencyCode CHAR(3),
            oneWay BOOLEAN,
            MINIMUM NUMERIC(10, 2),
            FIRST NUMERIC(10, 2),
            MEDIUM NUMERIC(10, 2),
            THIRD NUMERIC(10, 2),
            MAXIMUM NUMERIC(10, 2)
        )
        SORTKEY (
            departureDate
        );

        DROP TABLE IF EXISTS {schema}.dim_flight_prices;
        CREATE TABLE {schema}.dim_flight_prices (
            origin VARCHAR(20) distkey,
            destination VARCHAR(20),
            departureDate DATE,
            currencyCode CHAR(3),
            oneWay BOOLEAN,
            MINIMUM NUMERIC(10, 2),
            FIRST NUMERIC(10, 2),
            MEDIUM NUMERIC(10, 2),
            THIRD NUMERIC(10, 2),
            MAXIMUM NUMERIC(10, 2),
            PRIMARY KEY (origin, destination, departureDate)
        )
        SORTKEY (
            departureDate
        );
    """
)

# Cargamos nuestro Dataframe en nuestra tabla de Stagging, y luego con la sentencia MERGE cargamos toda la info a nuestra tabla dimensión.
with engine.connect() as conn, conn.begin():

    conn.execute("TRUNCATE TABLE stg")
    flights.to_sql(
    name="stg",
    con=conn,
    schema=schema,
    if_exists="append",
    method="multi",
    index=False,
    )

    conn.execute("""
    MERGE INTO dim_flight_prices
    USING stg
    ON dim_flight_prices.origin = stg.origin AND dim_flight_prices.destination = stg.destination AND dim_flight_prices.departureDate = stg.departureDate
    WHEN MATCHED THEN
        UPDATE SET
            origin = stg.origin,
            destination = stg.destination,
            departureDate = stg.departureDate,
            currencyCode = stg.currencyCode,
            oneWay = stg.oneWay,
            MINIMUM = stg.MINIMUM,
            FIRST = stg.FIRST,
            MEDIUM = stg.MEDIUM,
            THIRD = stg.THIRD,
            MAXIMUM = stg.MAXIMUM
    WHEN NOT MATCHED THEN
        INSERT (origin, destination, departureDate, currencyCode, oneWay, MINIMUM, FIRST, MEDIUM, THIRD, MAXIMUM)
        VALUES (stg.origin, stg.destination, stg.departureDate, stg.currencyCode, stg.oneWay, stg.MINIMUM, stg.FIRST, stg.MEDIUM, stg.THIRD, stg.MAXIMUM)
""")

