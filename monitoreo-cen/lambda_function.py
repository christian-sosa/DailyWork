import boto3
import pandas as pd
from datetime import datetime
import awswrangler as wr

NOW = datetime.utcnow()
START_OF_DAY = datetime(NOW.year, NOW.month, NOW.day, 0, 0, 0)

#
def get_sfn_data():

    client = boto3.client("stepfunctions")

    arn_list = {
        "arn:aws:states:us-east-1:233669286383:stateMachine:CEN-SF-EMPRESA-ELECTRICA-prd": "CEN-SF-EMPRESA-ELECTRICA",
        "arn:aws:states:us-east-1:233669286383:stateMachine:CEN-SF-ESTADOS-OPERATIVOS-prd": "CEN-SF-ESTADOS-OPERATIVOS",
        "arn:aws:states:us-east-1:233669286383:stateMachine:CEN-SF-INSTALACIONES-prd": "CEN-SF-INSTALACIONES",
        "arn:aws:states:us-east-1:233669286383:stateMachine:CEN-SF-DELTA-prd": "CEN-SF-DELTA",
    }

    data = []
    for arn in arn_list:
        response = client.list_executions(
            stateMachineArn=arn,
            maxResults=1,
        )

        data.append(response["executions"])

    df = pd.DataFrame([x[0] for x in data])
    df["nombre_proceso"] = df.stateMachineArn.map(arn_list)
    df = wr.catalog.sanitize_dataframe_columns_names(df)

    df["Statemachine"] = df.statemachinearn.str.split(":").str[-1]

    df["Duración"] = df.stopdate - df.startdate
    df = df[
        [
            "nombre_proceso",
            "Statemachine",
            "startdate",
            "stopdate",
            "Duración",
            "status",
        ]
    ]
    df = df.rename(
        columns={
            "nombre_proceso": "Nombre Proceso / Fuente",
            "status": "Estado",
            "startdate": "Inicio",
            "stopdate": "Fin",
        }
    )

    df.Estado = df.Estado.str.replace("SUCCEEDED", "✅", regex=True)
    df.Estado = df.Estado.str.replace("FAILED", "❌", regex=True)
    df.Estado = df.Estado.str.replace("ABORTED", "⊗", regex=True)
    df.Estado = df.Estado.str.replace("RUNNING", "🏃", regex=True)

    return df


def get_lambda_metrics():
    cloudwatch = boto3.resource("cloudwatch")
    metric = cloudwatch.Metric("AWS/Lambda", "Errors")

    lambda_names = {
        "arn:aws:lambda:us-east-1:233669286383:function:cen-ingesta-calidad-dato-lambda-distribution_s3_raw-prd": "cen-ingesta-calidad-dato-lambda-distribution_s3_raw-prd",
        "arn:aws:lambda:us-east-1:233669286383:function:cen-ingesta-calidad-dato-distribution_s3_raw_infotecnica-prd": "cen-ingesta-calidad-dato-distribution_s3_raw_infotecnica-prd",
    }
    metrics = []

    for name, nombre_proceso in lambda_names.items():
        response = metric.get_statistics(
            StartTime=START_OF_DAY,
            EndTime=NOW,
            Period=60 * 60 * 24,
            Dimensions=[
                {
                    "Name": "FunctionName",
                    "Value": name,
                },
            ],
            Statistics=["Sum"],
        )
        data = response.get("Datapoints", [])
        if len(data) == 0:
            continue
        data = data[0]
        data["Nombre Proceso"] = nombre_proceso
        data["Nombre Función"] = name
        metrics.append(data)

    df = pd.DataFrame(metrics)
    df["Timestamp"] = df.Timestamp.dt.date
    df.loc[df.Sum > 0, "status"] = "❌"
    df = df.fillna({"status": "✅"})
    df = df.rename(
        columns={
            "Timestamp": "Fecha de ejecución",
            "Sum": "Cantidad de errores",
            "status": "Estado",
        }
    )
    df = df.drop(columns=["Unit"])

    df = df[
        [
            "Nombre Proceso",
            "Nombre Función",
            "Cantidad de errores",
            "Fecha de ejecución",
            "Estado",
        ]
    ]

    return df


def send_email(
    body: str,
    to_addresses: list,
    cc_addresses: list,
    title: str,
    sender: str,
    bcc_addresses: str,
):
    ses_client = boto3.client("ses")

    response = ses_client.send_email(
        Source=sender,
        Destination={
            "ToAddresses": to_addresses,
            "CcAddresses": cc_addresses,
            "BccAddresses": bcc_addresses,
        },
        Message={
            "Subject": {"Data": title, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": title, "Charset": "UTF-8"},
                "Html": {"Data": body, "Charset": "UTF-8"},
            },
        },
    )

    return response


def get_body(sfn_table: str) -> str:
    tTemplate = f"""
        <html>
            <head>
                <title>Estado de las ejecuciones diarias CChC</title>
                <head>
                    <style>
                        table {{
                            border-collapse: collapse;
                        }}
                        td {{
                            padding-top: 0.2rem;
                            padding-bottom: 0.2rem;
                            padding-left: 0.5rem;
                            padding-right: 0.5rem;
                        }}
                        th {{
                            padding-left: 0.5rem;
                            padding-right: 0.5rem;
                            padding-top: 0.2rem;
                            padding-bottom: 0.2rem;
                        }}
                    </style>
                </head>
            </head>
            <body>
            <p>Se adjunta el estado de las ejecuciones diarias</p>
            <h1>Step functions</h1>
            { sfn_table }
        </body></html>
        """

    return tTemplate


def lambda_handler(event, context):
    # lambda_table = get_lambda_metrics()
    sfn_table = get_sfn_data()

    body = get_body(sfn_table=sfn_table.to_html(index=False, justify="center"))
    title = f"Ejecuciones día { NOW.strftime('%d-%m-%Y') }"
    send_email(
        bcc_addresses=[],
        to_addresses=[
            "csosa@arkho.io",
            "cpetry@arkho.io",
            "jtrespalacios@arkho.io",
            "dbarba@arkho.io",
            "daquino@arkho.io",
        ],
        cc_addresses=[],
        body=body,
        sender="csosa@arkho.io",
        title=title,
    )

    return "ok"
