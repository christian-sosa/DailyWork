import json
import datetime as dt
import boto3
import awswrangler as wr
import os


def read_excel(origin, sheet):
    read = wr.s3.read_excel(origin, sheet_name=sheet, engine="openpyxl")
    return read


def lambda_handler(event, context):
    dynamoDBTable = os.environ["dynamoDBTable"]
    # Tabla de Dynamo donde se encuentran los parámetros de las tablas
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.resource("s3")

    element = "costo_marginal_proyectado"
    table = dynamodb.Table(dynamoDBTable)

    # Lectura del elemento en la tabla de Dynamo:
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")

    data = config[
        "params"
    ]  # Dentro de "params" se encuentran todos los parámetros necesarios para trabajar con la tabla antes listada.
    data_params = json.loads(data)  # Conversión de json a lista

    # object_summary.key de destino del archivo a escribir:
    S3_origin = data_params.get("S3_origin")
    S3_origin = S3_origin + str(dt.datetime.now().year - 2) + "/"
    write_mode = data_params.get("write_mode")
    database = data_params.get("database")
    print("write write_mode: ", write_mode)
    print("S3_origin: ", S3_origin)
    print("database: ", database)

    bucketRaw = data_params.get("bucketRaw")
    bucketStaging = data_params.get("bucketStaging")

    Boto3bucketRaw = s3.Bucket(bucketRaw)

    for object_summary in Boto3bucketRaw.objects.filter(Prefix=S3_origin):
        print("objeto: ", object_summary.key)
        if ".xlsx" in object_summary.key:
            pathFix = f"s3://{bucketRaw}/{object_summary.key}"
            df = wr.s3.read_excel(pathFix, sheet_name="Sheet1", engine="openpyxl")

            df["year_partition"] = str(dt.date.today().year - 2)
            escenario = object_summary.key.split("/")[3]
            df["escenario_partition"] = escenario
            table = object_summary.key.split("/")[4].split("_")[0]

            pathFinal = f"s3://{bucketStaging}/planificacion-y-desarrollo/costo_marginal_proyectado/{table}/"
            print(pathFinal)
            wr.s3.to_parquet(
                df=df,
                path=pathFinal,
                index=False,
                dataset=True,
                compression="gzip",
                mode=write_mode,
                database=database,
                table=table,
                partition_cols=["year_partition", "escenario_partition"],
            )
