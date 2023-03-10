import boto3
import os
import json
import awswrangler as wr
import datetime

# Funcion para escritura del dataframe:
def df_write(df, dest, ext, element, write_mode, database):
    if ext == "parquet":
        wr.s3.to_parquet(
            df=df,
            path=dest,
            index=False,
            dataset=True,
            database=database,
            table=element,
            compression="gzip",
            mode=write_mode,
            partition_cols=["year"],
        )


# Función que agrega columnas de año, mes y día a partir de una columna de fecha.
def add_partition_fields(df, formato="%Y", col="ano"):
    df["year"] = [x for x in df[col]]
    return df


# Funcion para verficar si es nulo
def df_tasks(df, tasks):
    if (
        "del" in tasks
    ):  # Esta porcion de codigo, borra las filas de las columnas especificadas
        columns = tasks["del"]
        columns = columns.split(",")
        print("columnas: ", columns)
        if len(columns) > 0:
            df2 = df[
                df.tipo_plp != "X"
            ]  # Elimino todas las filas que contengan X en la columna tipo_plp
    return df2


def lambda_handler(event, context):
    dynamoDBTable = os.environ["dynamoDBTable"]
    element = os.environ["element"]

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(dynamoDBTable)

    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")
    data = config[
        "params"
    ]  # Dentro de "params" se encuentran todos los parámetros necesarios para trabajar con la tabla antes listada.
    data_params = json.loads(data)  # Conversión de json a lista

    # Base de datos del catálogo de glue a escribir:
    database = data_params.get("database_analytics")
    # Ruta de origen del archivo a leer:
    S3_origin = data_params.get("S3_origin_analytics")
    # Ruta de destino del archivo a escribir:
    S3_dest = data_params.get("S3_dest_analytics")
    # Extension del archivo a leer
    format_in = data_params.get("format_in")
    # Extension del archivo a escribir
    format_out = data_params.get("format_out")
    # Tipo de escritura: (overwrite_partitions, overwrite, append)
    write_mode = data_params.get("write_mode")
    # FACT
    element = "fact_" + element
    # Tareas a realizar sobre el dataframe
    tasks = data_params.get("tasks")
    tasks = tasks[0]

    # ----------------------------------------------------------------------------------------------------------------
    #                                      Principal
    # ----------------------------------------------------------------------------------------------------------------
    try:
        df1 = df_read(S3_origin, format_in)
        df1 = df_tasks(df1, tasks)

        df2 = df1["escenario"]
        df2.drop_duplicates(inplace=True)
        dict = df2.to_dict()
        escenarios = list(dict.values())

        df3 = df1["region"]
        df3.drop_duplicates(inplace=True)
        dict = df3.to_dict()
        regiones = list(dict.values())

        df4 = df1["tecnologia"]
        df4.drop_duplicates(inplace=True)
        dict = df4.to_dict()
        tecnologias = list(dict.values())
        print(tecnologias)

        df5 = df1["ano"]
        df5.drop_duplicates(inplace=True)
        anho_min = df5.min()
        anho_max = df5.max()

        for i in escenarios:
            for x in regiones:
                for y in tecnologias:
                    for anho in range(anho_min, anho_max + 1):

                        if df1.loc[
                            (df1["escenario"] == i)
                            & (df1["region"] == x)
                            & (df1["tecnologia"] == y)
                            & (df1["ano"] == anho)
                        ].empty:
                            new_row = {
                                "escenario": i,
                                "region": x,
                                "tecnologia": y,
                                "potencia": 0,
                                "ano": anho,
                            }
                            df1 = df1.append(new_row, ignore_index=True)
        df1 = df1.groupby(
            ["escenario", "region", "tecnologia", "ano"], as_index=False
        ).sum()
        df2 = add_partition_fields(df1)
        df_write(df2, S3_dest, format_out, element, write_mode, database)
    except:
        print("Ocurrió un error durante la ejecucion")
