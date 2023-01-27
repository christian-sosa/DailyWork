""""
import boto3

client = boto3.client("lambda")

response = client.list_invocations(
    FunctionName="my-lambda-function",
)

print(response["Invocations"])

"""

import boto3

# Inicializar cliente de Lambda
lambda_client = boto3.client("lambda")

# Nombre de la función Lambda
function_name = "cchc-dw-borrable-turn-on-rds-ec2"

# Obtener las últimas ejecuciones de la función
response = lambda_client.list_events(FunctionName=function_name, MaxItems=10)

# Imprimir los detalles de las ejecuciones
for event in response["Events"]:
    print(event["Timestamp"], event["InvokedFunctionArn"])
