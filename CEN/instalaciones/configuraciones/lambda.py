import awswrangler as wr
import pandas as pd
import boto3
import json
import os


def df_write(df, dest, element, write_mode, database):
    wr.s3.to_parquet(
        df=df,
        path=dest,
        index=False,
        dataset=True,
        compression="gzip",
        mode=write_mode,
        database=database,
        table=element,
        schema_evolution=True,
        sanitize_columns=False,
    )


def sendSNS(error):
    sns = boto3.client("sns")
    env = os.environ["stage"]
    topic = os.environ["topic"]
    print("env:", env)
    print("topic:", topic)

    FinalMessage = f"Instalaciones Configuraciones:  Ambiente: {env} ,Error: {error}"
    print("mensaje final", FinalMessage)
    sendError = sns.publish(TopicArn=topic, Message=FinalMessage)


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    tabla = os.environ["dynamoDB"]
    table = dynamodb.Table(tabla)
    element = os.environ["element"]

    # Lectura del elemento en la tabla de Dynamo:
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")

    pd.set_option("display.max_columns", None)

    #### Lectura de los parametros del json de dynamodb
    try:
        data = config["params"]
        catalog_id = config["catalog_id"]
        data_params = json.loads(data)

        S3_origin_configuraciones = data_params.get("S3_origin_configuraciones")
        S3_origin_Datos_Tecnicos = data_params.get("S3_origin_Datos_Tecnicos")
        S3_origin_FT_Aplicada_Datos = data_params.get("S3_origin_FT_Aplicada_Datos")
        S3_origin_FT_Aplicadas = data_params.get("S3_origin_FT_Aplicadas")
        S3_origin_FT_Estandar_Datos = data_params.get("S3_origin_FT_Estandar_Datos")
        S3_origin_Centrales = data_params.get("S3_origin_Centrales")
        S3_origin_Conceptos = data_params.get("S3_origin_Conceptos")
        S3_origin_Instalaciones = data_params.get("S3_origin_Instalaciones")
        S3_origin_Empresas = data_params.get("S3_origin_Empresas")
        S3_origin_Grupos = data_params.get("S3_origin_Grupos")
        ruta_destino = data_params.get("ruta_destino")
        table = data_params.get("table")
        write_mode = data_params.get("write_mode")
        database = data_params.get("database")
    except:
        print("Fallo la obtencion de datos de DYNAMO")
        sendSNS("Fallo la obtencion de datos de DYNAMO")
    # Lectura de dataframes

    try:
        df_origin_configuraciones = wr.s3.read_parquet(
            path=S3_origin_configuraciones,
            columns=[
                "id_configuracion",
                "id_central",
                "configuracionnombre",
                "configuracionnumero",
                "configuracionnemotecnico",
                "configuraciondescripcion",
            ],
        )
        df_origin_Datos_Tecnicos = wr.s3.read_parquet(
            path=S3_origin_Datos_Tecnicos, columns=["id_datotecnico", "id_concepto"]
        )
        df_origin_FT_Aplicada_Datos = wr.s3.read_parquet(
            path=S3_origin_FT_Aplicada_Datos,
            columns=["fta_datovalortexto", "id_fte_dato", "id_ftaplicada"],
        )
        df_origin_FT_Aplicadas = wr.s3.read_parquet(
            path=S3_origin_FT_Aplicadas, columns=["id_ftaplicada", "id_instalacion"]
        )
        df_origin_FT_Estandar_Datos = wr.s3.read_parquet(
            path=S3_origin_FT_Estandar_Datos, columns=["id_fte_dato", "id_datotecnico"]
        )
        df_origin_Centrales = wr.s3.read_parquet(
            path=S3_origin_Centrales,
            columns=[
                "id_central",
                "centralnombre",
                "id_propietario",
                "id_coordinado",
                "id_centrocontrol",
            ],
        )
        df_origin_Conceptos = wr.s3.read_parquet(
            path=S3_origin_Conceptos, columns=["id_concepto", "conceptonombre"]
        )
        df_origin_Instalaciones = wr.s3.read_parquet(
            path=S3_origin_Instalaciones,
            columns=["id_configuracion", "id_instalacion", "id_instalaciontipo"],
        )
        df_origin_Grupos = wr.s3.read_parquet(
            path=S3_origin_Grupos, columns=["id_grupo", "gruponombre"]
        )
        df_origin_Empresas = wr.s3.read_parquet(
            path=S3_origin_Empresas, columns=["id_empresa", "empresanombre"]
        )
        df_origin_Empresas2 = df_origin_Empresas.copy()
    except:
        print("Fallo la lectura de los dataframes")
        sendSNS("Fallo la lectura de los dataframes")

    try:

        # Renombrando columnas de empresa
        df_origin_Centrales.rename(columns={"id_central": "id_central3"}, inplace=True)
        df_origin_FT_Aplicadas.rename(
            columns={
                "id_ftaplicada": "id_ftaplicada2",
                "id_instalacion": "id_instalacion2",
            },
            inplace=True,
        )
        df_origin_Instalaciones.rename(
            columns={"id_configuracion": "id_configuracion2"}, inplace=True
        )
        df_origin_Datos_Tecnicos.rename(
            columns={
                "id_datotecnico": "id_datotecnico2",
                "id_concepto": "id_concepto2",
            },
            inplace=True,
        )
        df_origin_FT_Aplicada_Datos.rename(
            columns={"id_fte_dato": "id_fte_dato2"}, inplace=True
        )
        df_origin_Empresas2.rename(
            columns={"empresanombre": "empresanombre2"}, inplace=True
        )
        # Aplicando filtros
        df_origin_Instalaciones = df_origin_Instalaciones[
            df_origin_Instalaciones.id_instalaciontipo == 5
        ]

        # JOINS MIOS
        df = df_origin_configuraciones.join(
            df_origin_Centrales.set_index("id_central3"), on="id_central", how="left"
        )
        df = df.join(
            df_origin_Instalaciones.set_index("id_configuracion2"),
            on="id_configuracion",
            how="left",
        )
        df = df.join(
            df_origin_FT_Aplicadas.set_index("id_instalacion2"),
            on="id_instalacion",
            how="left",
        )
        df = df.join(
            df_origin_FT_Aplicada_Datos.set_index("id_ftaplicada"),
            on="id_ftaplicada2",
            how="left",
        )
        df = df.join(
            df_origin_FT_Estandar_Datos.set_index("id_fte_dato"),
            on="id_fte_dato2",
            how="left",
        )
        df = df.join(
            df_origin_Datos_Tecnicos.set_index("id_datotecnico2"),
            on="id_datotecnico",
            how="left",
        )
        df = df.join(
            df_origin_Conceptos.set_index("id_concepto"), on="id_concepto2", how="left"
        )
        df = df.join(
            df_origin_Empresas.set_index("id_empresa"),
            on="id_centrocontrol",
            how="left",
        )
        df = df.join(
            df_origin_Empresas2.set_index("id_empresa"), on="id_propietario", how="left"
        )
        df = df.join(
            df_origin_Grupos.set_index("id_grupo"), on="id_coordinado", how="left"
        )

        df2 = df[
            [
                "id_configuracion",
                "configuracionnombre",
                "configuracionnumero",
                "centralnombre",
                "id_central",
                "gruponombre",
                "id_coordinado",
                "empresanombre",
                "id_centrocontrol",
                "configuracionnemotecnico",
                "configuraciondescripcion",
            ]
        ]

        # Aplicando filtros
        df = df[
            df["id_concepto2"].isin(
                [
                    1464,
                    1465,
                    1768,
                    1780,
                    1466,
                    1467,
                    1477,
                    1478,
                    1479,
                    1480,
                    1771,
                    1772,
                    1773,
                    1774,
                    1789,
                    2893,
                    2894,
                    2895,
                    2896,
                    3046,
                    3047,
                    3048,
                    3049,
                    3050,
                    3051,
                    791,
                    792,
                    1470,
                    1471,
                    1472,
                    1473,
                    1481,
                    1482,
                    1781,
                    1782,
                    1783,
                    1784,
                    1785,
                    1786,
                    1787,
                    1788,
                    3052,
                ]
            )
        ]

        # Pivoteando columnas
        df = df.pivot(
            index="configuracionnombre",
            columns="conceptonombre",
            values="fta_datovalortexto",
        )

        df = df2.join(df, on="configuracionnombre", how="left")

        # autor daniel aquino
        df.drop_duplicates(inplace=True)

        print(df.dtypes)

        df.rename(
            columns={
                "id_configuracion": "id",
                "configuracionnombre": "nombre",
                "configuracionnumero": "numero",
                "centralnombre": "nombre central",
                "id_central": "id central",
                "gruponombre": "nombre coordinado",
                "id_coordinado": "id coordinado",
                "empresanombre": "nombre centro control",
                "id_centrocontrol": "id centro control",
                "configuracionnemotecnico": "nemotecnico",
                "configuraciondescripcion": "descripcion",
            },
            inplace=True,
        )

        df = df[
            [
                "id",
                "nombre",
                "numero",
                "nombre central",
                "id central",
                "nombre coordinado",
                "id coordinado",
                "nombre centro control",
                "id centro control",
                "nemotecnico",
                "descripcion",
                "Tipo de configuración de unidad generadora",
                "Sistema eléctrico",
                "Fecha entrada en operación comercial",
                "Fecha primera sincrocnización",
                "Potencia bruta máxima",
                "Consumos propios (SS/AA)",
                "Potencia neta máxima",
                "Carta Pmax",
                "Fecha emisión carta Pmax",
                "Potencia bruta mínima con CPF",
                "Potencia bruta mínima ambiental",
                "Potencia bruta mínima (A.T.)",
                "Potencia neta mínima excedentes que puede inyectar la Cogeneradora",
                "Carta Pmin",
                "Fecha emisión carta Pmin",
                "Tasa de toma de carga",
                "Tasa de bajada de carga",
                "Tiempo desde partida hasta sincronización",
                "Tiempo desde sincronización hasta mínimo técnico",
                "Tiempo desde mínimo técnico hasta potencia nominal",
                "Tiempo desde potencia nominal hasta mínimo técnico",
                "Tiempo desde mínimo técnico hasta desconexión",
                "Tiempo desde desconexión hasta apagado",
                "Carta PPyD",
                "Fecha emisión carta PPyD",
                "Ralación de acoplamiento",
                "Capacidad de estanque combustible líquido",
                "Autonomía sin recarga para potencia activa bruta máxima",
                "Autonomía sin recarga para potencia mínima técnica",
                "Tiempo máximo de enbancamiento",
                "Tiempo mínimo de embancamiento",
                "Consumo especifico neto",
                "Carta - Consumo Específico Neto (Carta - CEN)",
                "Fecha carta - Consumo Específico Neto (Fecha Carta - CEN)",
                "Capacidad operacional de estanque o embalse",
                "Cota máxima de estanque o embalse",
                "Cota máxima operacional de estanque o embalse",
                "Cota mínima operacional de estanque o embalse",
                "Cota mínima de estanque o embalse",
                "Curva de rendimiento",
                "Rendimiento medio",
                "Potencia bruta máxima según cota de estanque o embalse",
            ]
        ]

        wr.s3.to_parquet(
            df=df,
            path=ruta_destino,
            index=False,
            dataset=True,
            compression="gzip",
            mode=write_mode,
            database=database,
            table=table,
            schema_evolution=True,
            sanitize_columns=False,
        )
    except:
        print("Fallaron los cruces de datos")
        sendSNS("Fallaron los cruces de datos")
