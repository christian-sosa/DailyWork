import awswrangler as wr
import pandas as pd
import boto3
import datetime as dt
import json


def add_partition_fields(df, formato="%Y", col="year"):
    df["year"] = [x for x in df[col]]
    return df


def lambda_handler(event, context):
    dynamoDBTable = "cen-ingesta-calidad-dato-dev-config"
    element = "demanda_proyectada"
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(dynamoDBTable)

    # Lectura del elemento en la tabla de Dynamo:
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")
    data = config[
        "params"
    ]  # Dentro de "params" se encuentran todos los parámetros necesarios para trabajar con la tabla antes listada.
    data_params = json.loads(data)

    ruta_origen_medio = data_params.get("ruta_origen_medio")
    ruta_origen_alto = data_params.get("ruta_origen_alto")
    ruta_destino = data_params.get("ruta_destino")
    table = data_params.get("table")
    write_mode = data_params.get("write_mode")
    database_analytics = data_params.get("database_analytics")
    # cen-ingesta.calidad-staging-dev

    particion = str(dt.datetime.now().year - 2)
    print(particion)

    ruta_origen_alto = ruta_origen_alto + "year=" + particion + "/"
    ruta_origen_medio = ruta_origen_medio + "year=" + particion + "/"

    print(ruta_origen_medio)
    print(ruta_origen_alto)

    df_medio = wr.s3.read_parquet(ruta_origen_medio)
    df_alto = wr.s3.read_parquet(ruta_origen_alto)

    #### Cambiando nombre a columnas
    columns_medio_alto = {
        "barra_proyeccion": "nombre_barra_proyeccion",
        "nombre_barra": "nombre_barra",
        "ano": "year",
        "tag_mes": "nombre_mes",
        "MedidaHoraria2": "medida_horaria",
        "reg_rom_": "registro_romano",
        "reg_num": "registro_numerico",
    }
    df_medio.rename(columns=columns_medio_alto, inplace=True)
    df_alto.rename(columns=columns_medio_alto, inplace=True)
    df_medio["escenario"] = "escenario_medio"
    df_alto["escenario"] = "escenario_alto"
    df_resultado = pd.concat([df_medio, df_alto])
    df_resultado["anio"] = df_resultado["year"].astype(str)
    df_resultado = add_partition_fields(df_resultado)
    df_resultado["yearParticion"] = particion
    wr.s3.to_parquet(
        df=df_resultado,
        path=ruta_destino,
        index=False,
        dataset=True,
        compression="gzip",
        mode=write_mode,
        database=database_analytics,
        table=table,
        partition_cols=["yearParticion", "year"],
    )
