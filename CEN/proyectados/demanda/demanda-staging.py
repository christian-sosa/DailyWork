import json
import boto3
import awswrangler as wr
import os


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    s3 = boto3.resource("s3")
    dynamoDBTable = os.environ["dynamoDBTable"]

    element = os.environ["demanda_proyectada"]
    print(element)
    table = dynamodb.Table(dynamoDBTable)
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")

    data = config["params"]
    data_params = json.loads(data)

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

    sheet_name = "Sheet1"
    for object_summary in Boto3bucketRaw.objects.filter(Prefix=S3_origin):
        if ".xlsx" in object_summary.key:
            pathFix = f"s3://{bucketRaw}/{object_summary.key}"
            df = wr.s3.read_excel(
                pathFix,
                sheet_name=sheet_name,
                dtype={
                    "BARRA_PROYECCION": "string",
                    "nombre_barra": "string",
                    "Año": "int",
                    "Tag_Mes": "string",
                    "Mes": "int",
                    "tipo1": "string",
                    "sector_economico": "string",
                    "comuna": "string",
                    "region": "string",
                    "Tasa": "double",
                    "MedidaHoraria2": "double",
                    "Demanda": "double",
                    "Modelo": "string",
                    "Empresa": "string",
                    "BARRA_PLP": "string",
                    "BARRA_PCP": "string",
                    "BARRA_PLEXOS": "string",
                    "BARRA_PLEXOS_21": "string",
                    "Factor_bisiesto": "int",
                    "Reg. Rom.": "string",
                    "Reg. Num": "string",
                },
            )

            df["year"] = dest[2]
            dest = object_summary.key.split("/")[:-1]
            table = dest[3]
            pathDest = f"s3://{bucketStaging}/{dest[0]}/{dest[1]}/{table}/"
            print(pathDest)

            wr.s3.to_parquet(
                df=df,
                path=pathDest,
                index=False,
                dataset=True,
                compression="gzip",
                mode=write_mode,
                database=database,
                table=table,
                partition_cols=["year"],
            )
