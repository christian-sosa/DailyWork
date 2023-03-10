import json
import datetime as dt
import boto3
import awswrangler as wr
import os


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.resource("s3")
    dynamoDBTable = os.environ["dynamoDBTable"]

    element = "oferta_proyectada"
    print(element)
    table = dynamodb.Table(dynamoDBTable)
    # Lectura del elemento en la tabla de Dynamo:
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")

    data = config[
        "params"
    ]  # Dentro de "params" se encuentran todos los parámetros necesarios para trabajar con la tabla antes listada.
    data_params = json.loads(data)  # Conversión de json a lista

    # Ruta de destino del archivo a escribir:
    S3_origin = data_params.get("S3_origin")
    write_mode = data_params.get("write_mode")
    database = data_params.get("database")
    print("S3_origin: ", S3_origin)
    print("write_mode: ", write_mode)
    print("database: ", database)

    bucketRaw = os.environ["bucketRaw"]
    bucketStaging = os.environ["bucketStaging"]

    Boto3bucketRaw = s3.Bucket(bucketRaw)

    sheet_name_obras = data_params.get("sheet_name_obras")
    table = "Plan_de_Obras"

    for object_summary in Boto3bucketRaw.objects.filter(Prefix=S3_origin):
        if "Plan_de_Obras.xlsx" in object_summary.key:
            pathFix = f"s3://{bucketRaw}/{object_summary.key}"
            pathFinal = f"s3://{bucketStaging}/planificacion-y-desarrollo/oferta_proyectada/{table}/"

            df = wr.s3.read_excel(
                pathFix,
                sheet_name=sheet_name_obras,
                dtype={
                    "tecnología": "string",
                    "región": "string",
                    "Nombre PLP": "string",
                    "potencia": "int64",
                    "Barra PLP": "string",
                    "Tipo PLP": "string",
                    "año": "int64",
                    "escenario": "string",
                },
            )
            df["year"] = str(dt.date.today().year - 2)
            wr.s3.to_parquet(
                df=df,
                path=pathFinal,
                index=False,
                dataset=True,
                compression="gzip",
                mode=write_mode,
                database=database,
                table="plan_de_obras",
                partition_cols=["year"],
            )
