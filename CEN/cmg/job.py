import awswrangler as wr
import pandas as pd
import sys
from awsglue.utils import getResolvedOptions
import boto3
from datetime import datetime, timedelta
import utils as utils

pd.set_option("display.max_columns", None)


def get_year(record):
    dt = datetime.strptime(record, "%Y-%m-%d")
    return dt.year


def get_month(record):
    dt = datetime.strptime(record, "%Y-%m-%d")
    return dt.month


def get_day(record):
    dt = datetime.strptime(record, "%Y-%m-%d")
    return dt.day


def add_partition_fields(df):
    df["year"] = [get_year(x) for x in df["FECHA_REPORTE"]]
    df["month"] = [get_month(x) for x in df["FECHA_REPORTE"]]
    df["day"] = [get_day(x) for x in df["FECHA_REPORTE"]]
    df = df.drop(["FECHA_REPORTE"], axis=1)
    return df


# Funcion para escribir log en dinamo
def write_count_dynamo(df, S3, table, database):
    Count = len(df.index)
    print(Count)
    ERROR = None  # Inicialización del error.
    STATUS = True
    ambiente = database.split("_")
    stage = ambiente[1]
    if Count < 1:
        ERROR = "Failed: bucket empty"
        STATUS = False
        # save_log(stage, status, error, quantity, path, prefix):
    utils.save_log(database, STATUS, ERROR, Count, S3, table, stage)


mandatory_params = ["TABLE_NAME", "CONFIG_KEY"]

if "--DAY" in sys.argv and "--MONTH" in sys.argv and "--YEAR" in sys.argv:
    mandatory_params.append("YEAR")
    mandatory_params.append("MONTH")
    mandatory_params.append("DAY")
    args = getResolvedOptions(sys.argv, mandatory_params)
    dates = [f'year={args["YEAR"]}/month={args["MONTH"]}/day={args["DAY"]}']
else:
    args = getResolvedOptions(sys.argv, mandatory_params)
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    two_days_ago = today - timedelta(days=2)
    dates = [
        f"year={today.year}/month={today.month}/day={today.day}",
        f"year={yesterday.year}/month={yesterday.month}/day={yesterday.day}",
        f"year={two_days_ago.year}/month={two_days_ago.month}/day={two_days_ago.day}",
    ]

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(args["TABLE_NAME"])

# Lectura del elemento en la tabla de Dynamo:
config = table.get_item(Key={"CEN": args["CONFIG_KEY"]}).get("Item")

for date in dates:

    try:
        # 1. Lectura fuentes en dataframe
        df_topology_key = wr.s3.read_parquet(config["topology_key_origin"])
        df_prog_program = wr.s3.read_parquet(config["prog_keyprogram_origin"] + date)

        # topology_key
        df_topology_key = df_topology_key[["id", "key_type_id", "name"]]

        # prog_keyprogram
        df_prog_program = df_prog_program[
            ["id", "date", "key_id", "value", "reported_time_id", "audit_id"]
        ]

        columns_gen = {"id": "id_prog", "key_id": "key_id", "date": "date_partition"}
        df_prog_program = df_prog_program.rename(columns=columns_gen, inplace=False)

        # 3. filter
        columns_datatype = {"key_type_id": "int64"}
        df_topology_key = df_topology_key.astype(columns_datatype)
        df_key_filtered = df_topology_key[df_topology_key["key_type_id"] == 92]

        # 4. Join tables
        df_genprog0 = df_prog_program.join(
            df_key_filtered.set_index("id"), on="key_id", how="inner"
        )

        # 5. filter columns
        df_gen = df_genprog0[
            [
                "key_id",
                "key_type_id",
                "reported_time_id",
                "value",
                "date_partition",
                "name",
                "audit_id",
            ]
        ]

        print(df_gen.head())

        # 6. rank

        df_gen["audit_id_rank"] = df_gen.groupby(["name", "reported_time_id"])[
            "audit_id"
        ].rank(method="first")

        # filtrar por el

        print(df_gen.head())
        df_gen = df_gen[df_gen["audit_id_rank"] == 1]

        # 6.5 remove audit_id_rank

        df_gen = df_gen[
            ["key_id", "key_type_id", "reported_time_id", "value", "date_partition"]
        ]

        print(df_gen.head())

        # 7. Rename columns
        columns_gen = {
            "key_id": "ID_BARRA",
            "key_type_id": "ID_CONCEPTO_UNIDAD",
            "reported_time_id": "ID_TIEMPO",
            "value": "V_COSTOMARGINALPROGRAMADO",
            "date_partition": "FECHA_REPORTE",
        }

        fact_df_gen = df_gen.rename(columns=columns_gen, inplace=False)

        # 9. Create partitions and parquet
        write_path = config["cost_marginal_mc_destination"]

        fact_df_gen = add_partition_fields(fact_df_gen)

        write_count_dynamo(
            fact_df_gen, write_path, config["table_name"], config["database"]
        )

        wr.s3.to_parquet(
            df=fact_df_gen,
            path=write_path,
            index=False,
            dataset=True,
            compression="gzip",
            mode=config["mode"],
            database=config["database"],
            table=config["table_name"],
            catalog_id=config["catalog_id"],
            schema_evolution=True,
            sanitize_columns=False,
            partition_cols=["year", "month", "day"],
        )
 
    except:
        sns = boto3.client('sns')
        file = config["prog_keyprogram_origin"] + date

        mandatory = []
        if "--ENV" in sys.argv and "--TOPIC" in sys.argv:
            mandatory.append('ENV')
            mandatory.append('TOPIC')
            print(mandatory)
            args = getResolvedOptions(sys.argv, mandatory)
            print(args)
            env = args['ENV']
            topic = args['TOPIC']
            print("env:",env)
            print("topic:",topic)
            print("Ruta: ",file)

        FinalMessage = f"CostoMarginal:  Ambiente: {env} ,Error: No existe el archivo {file},  message: No existe el archivo {file}"
    
        print("mensaje final", FinalMessage)
        
        sendError = sns.publish(
            TopicArn=topic,
            Message=FinalMessage
            )

