import awswrangler as wr
import pandas as pd
import boto3
import json
import os


def sendSNS(error):
    sns = boto3.client("sns")
    env = os.environ["stage"]
    topic = os.environ["topic"]
    print("env:", env)
    print("topic:", topic)

    FinalMessage = f"Instalaciones Subestaciones:  Ambiente: {env} ,Error: {error}"
    print("mensaje final", FinalMessage)
    sns.publish(TopicArn=topic, Message=FinalMessage)


def lambda_handler(event, context):
    dynamodb = boto3.resource("dynamodb")
    tabla = os.environ["dynamoDB"]
    table = dynamodb.Table(tabla)
    element = os.environ["element"]

    # Lectura del elemento en la tabla de Dynamo:
    config = table.get_item(Key={"CEN": "{}".format(element)}).get("Item")

    pd.set_option("display.max_columns", None)

    try:
        #### Lectura de los parametros del json de dynamodb
        data = config["params"]
        data_params = json.loads(data)

        S3_origin_Datos_Tecnicos = data_params.get("S3_origin_Datos_Tecnicos")
        S3_origin_FT_Aplicada_Datos = data_params.get("S3_origin_FT_Aplicada_Datos")
        S3_origin_FT_Aplicadas = data_params.get("S3_origin_FT_Aplicadas")
        S3_origin_FT_Estandar_Datos = data_params.get("S3_origin_FT_Estandar_Datos")
        S3_origin_Subestaciones = data_params.get("S3_origin_Subestaciones")
        S3_origin_Conceptos = data_params.get("S3_origin_Conceptos")
        S3_origin_Instalaciones = data_params.get("S3_origin_Instalaciones")
        S3_origin_Empresas = data_params.get("S3_origin_Empresas")
        S3_origin_Maestro_Usuarios = data_params.get("S3_origin_Maestro_Usuarios")
        S3_origin_Grupos = data_params.get("S3_origin_Grupos")
        ruta_destino = data_params.get("ruta_destino")
        table = data_params.get("table")
        write_mode = data_params.get("write_mode")
        database = data_params.get("database")

    except:
        print("Fallo la obtencion de datos de DYNAMO")
        sendSNS("Fallo la obtencion de datos de DYNAMO")

    try:
        # Lectura de dataframes
        df_origin_Subestaciones = wr.s3.read_parquet(
            path=S3_origin_Subestaciones,
            columns=[
                "id_subestacion",
                "subestacionnombre",
                "subestacionnumero",
                "subestacionnemotecnico",
                "subestaciondescripcion",
                "id_centrocontrol",
                "id_propietario",
                "id_coordinado",
                "subestacioncodigo",
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
        df_origin_Conceptos = wr.s3.read_parquet(
            path=S3_origin_Conceptos, columns=["id_concepto", "conceptonombre"]
        )
        df_origin_Instalaciones = wr.s3.read_parquet(
            path=S3_origin_Instalaciones,
            columns=["id_instalacion", "id_instalaciontipo", "id_subestacion"],
        )
        df_origin_Empresas = wr.s3.read_parquet(
            path=S3_origin_Empresas, columns=["id_empresa", "empresanombre"]
        )
        df_origin_maestro = wr.s3.read_parquet(
            path=S3_origin_Maestro_Usuarios, columns=["id_infotecnica", "razon_social"]
        )
        df_origin_Grupos = wr.s3.read_parquet(
            path=S3_origin_Grupos, columns=["id_grupo", "gruponombre"]
        )
        df_origin_Empresas2 = df_origin_Empresas.copy()
    except:
        print("Fallo la lectura de los dataframes")
        sendSNS("Fallo la lectura de los dataframes")

    try:
        # Renombrando columnas de empresa
        df_origin_FT_Aplicadas.rename(
            columns={
                "id_ftaplicada": "id_ftaplicada2",
                "id_instalacion": "id_instalacion2",
            },
            inplace=True,
        )
        df_origin_Instalaciones.rename(
            columns={"id_subestacion": "id_subestacion2"}, inplace=True
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
            df_origin_Instalaciones.id_instalaciontipo == 10
        ]
        df_origin_maestro["razon_social"] = df_origin_maestro[
            "razon_social"
        ].str.upper()
        df_origin_maestro = df_origin_maestro.drop_duplicates(subset=["razon_social"])

        df_origin_Subestaciones["id_coordinado"].fillna(-1, inplace=True)

        # JOINS MIOS
        df = df_origin_Subestaciones.join(
            df_origin_Instalaciones.set_index("id_subestacion2"),
            on="id_subestacion",
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
        df = df.join(
            df_origin_maestro.set_index("razon_social"), on="gruponombre", how="left"
        )

        df2 = df[
            [
                "id_subestacion",
                "subestacionnombre",
                "empresanombre",
                "empresanombre2",
                "gruponombre",
                "subestacionnumero",
                "subestacionnemotecnico",
                "subestaciondescripcion",
                "fta_datovalortexto",
                "conceptonombre",
                "id_concepto2",
                "subestacioncodigo",
                "id_centrocontrol",
                "id_propietario",
                "id_coordinado",
            ]
        ]

        df = df[
            df["id_concepto2"].isin(
                [
                    784,
                    930,
                    931,
                    1575,
                    1252,
                    1163,
                    1164,
                    1165,
                    982,
                    1166,
                    1167,
                    347,
                    1253,
                    1170,
                    1171,
                    1254,
                    759,
                    760,
                    769,
                    744,
                    745,
                    746,
                    747,
                    1156,
                ]
            )
        ]

        df.drop_duplicates(subset=["subestacionnombre", "conceptonombre"], inplace=True)
        df = df.pivot(
            index="subestacionnombre",
            columns="conceptonombre",
            values="fta_datovalortexto",
        )
        df = df2.join(df, on="subestacionnombre", how="left")

        df.rename(
            columns={
                "id_subestacion": "id",
                "subestacionnombre": "nombre",
                "empresanombre": "nombre centro control",
                "empresanombre2": "nombre propietario",
                "gruponombre": "empresa coordinada",
                "subestacionnumero": "numero",
                "subestacionnemotecnico": "nemotecnico",
                "subestaciondescripcion": "descripcion",
                "subestacioncodigo": "codigo_subestacion",
            },
            inplace=True,
        )
        print(df.dtypes)
        df = df[
            [
                "id",
                "nombre",
                "nombre centro control",
                "id_centrocontrol",
                "nombre propietario",
                "id_propietario",
                "empresa coordinada",
                "id_coordinado",
                "numero",
                "nemotecnico",
                "codigo_subestacion",
                "descripcion",
                "Región",
                "Provincia",
                "Comuna",
                "5.1 Barras por nivel de tensión y su respectiva capacidad térmica, en función de la T° ambiente y T° conductor (Tabla de Relación Corriente – Temperatura)",
                "5.2 Tipo de configuración (barra simple, barra doble, interruptor y medio, etc.)",
                "5.3 Equipos de transformación",
                "5.4 Interruptores (especificar si corresponden a interruptores de paño de línea, seccionadores de barra, de transferencia, u otros)",
                "5.5 Desconectadores",
                "5.6 Conexiones de puesta a tierra. Indicar si la subestación posee conexiones y sus ubicaciones",
                "5.7 Equipos de medida",
                "5.8 Equipos de sincronización",
                "5.9 Equipos de comunicaciones",
                "5.10 Sistemas de protección",
                "5.11 Transformadores de medida (corriente y tensión)",
                "5.12 Pararrayos",
                "5.13 Fecha de entrada en operación",
                "Coordenada Este",
                "Coordenada Norte",
                "Zona o Huso [Ej: 18H-19J etc.]",
                "1 Diagrama unilineal, señalando capacidad nominal de equipos primarios  (*.dwg y *.pdf)",
                "2 Plano de planta y elevación de la subestación (*dwg y *. pdf)",
                "3 Plano de la malla de tierra: aérea y subterránea (*dwg y *.pdf)",
                "4 Diagramas unilineales de los servicios auxiliares de CA y CC (*.dwg y *.pdf)",
                "5 Disposición de equipos en sala de servicios generales.",
            ]
        ]

        df.drop_duplicates(inplace=True)

        df["id_coordinado"] = df["id_coordinado"].astype(str)
        df["id_coordinado"] = df["id_coordinado"].replace("-1", "")

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
