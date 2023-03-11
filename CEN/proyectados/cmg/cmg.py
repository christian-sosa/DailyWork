import json
import awswrangler as wr
import boto3
import datetime as dt
import os

# Función que agrega columnas de año, mes y día a partir de una columna de fecha.
def add_partition_fields(df, formato="%Y", col="anio"):
    df["year"] = [x for x in df[col]]
    return df


def lambda_handler(event, context):
    dynamoDBTable = os.environ["dynamoDBTable"]
    element = os.environ["element"]
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(dynamoDBTable)

    # Lectura del elemento en la tabla de Dynamo:
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")
    data = config[
        "params"
    ]  # Dentro de "params" se encuentran todos los parámetros necesarios para trabajar con la tabla antes listada.
    data_params = json.loads(data)

    pet_origen = data_params.get("pet_origen")
    S3_origin = pet_origen + "year_partition=" + str(dt.datetime.now().year - 2) + "/"
    pet_destino = data_params.get("pet_destino")
    table = data_params.get("table")
    write_mode = data_params.get("write_mode")
    database_analytics = data_params.get("database_analytics")

    df = wr.s3.read_parquet(path=S3_origin)
    df["fecha"] = "01" + "-" + df["mes"].astype(str) + "-" + df["year"].astype(str)
    #### Cambiando nombre a columnas
    columns = {"hidro": "hidrologia", "year": "anio", "barnom": "nombre_barra"}

    df.rename(columns=columns, inplace=True)
    df = add_partition_fields(df)
    df["yearParticion"] = str(dt.datetime.now().year - 2)
    print(df.dtypes)
    wr.s3.to_parquet(
        df=df,
        path=pet_destino,
        index=False,
        dataset=True,
        compression="gzip",
        mode=write_mode,
        database=database_analytics,
        table=table,
        partition_cols=["yearParticion", "year"],
    )
