# Migrado de spark a pandas

import pandas as pd
import awswrangler as wr
from datetime import datetime
from dateutil import tz
import json
from cchc_commons import CChCHelper, JobStage, JobTypes, getGlueArgs


app = CChCHelper(
    job_name="libro_mayor_analytics",
    job_type=JobTypes.GLUEJOB,
    stage=JobStage.ANALYTICS,
)

args = getGlueArgs(["ambiente", "uuid", "sns_topic"], ["full_load", "fecha", "years"])
AMBIENTE = args["ambiente"]
fecha = args["fecha"]
CLT = tz.gettz("America/Santiago")

print("Utilizando argumentos")
print(json.dumps(args, indent=2, default=str))


import gc

if args.get("years") is None:
    args["years"] = str(fecha.year)


@app.perfme
@app.notify
def fact_resumen_financiero(year):
    ruta_resumen = f"s3://cchc-dw-{AMBIENTE}-staging/sap/mgmt/extraction_year={year}"
    ruta_cuentas = f"s3://cchc-dw-{AMBIENTE}-staging/tableros_regionales/cuentas_sap"
    ruta_camaras = f"s3://cchc-dw-{AMBIENTE}-analytics/dim_camaras_regionales"
    ruta_tiempo = f"s3://cchc-dw-{AMBIENTE}-analytics/dim_maestro_tiempo"
    ruta_moneda = f"s3://cchc-dw-{AMBIENTE}-analytics/moneda"
    ruta_dim_comites = f"s3://cchc-dw-{AMBIENTE}-analytics/dim_comites"
    ruta_codigos_sap = (
        f"s3://cchc-dw-{AMBIENTE}-staging/tableros_regionales/codigos_camara_sap"
    )

    ruta_salida = f"s3://cchc-dw-{AMBIENTE}-analytics/fact_resumen_financiero"

    resumen = wr.s3.read_parquet(ruta_resumen, dataset=True)
    cuentas = wr.s3.read_parquet(ruta_cuentas, dataset=True, columns=["codigo"])
    camaras = wr.s3.read_parquet(
        ruta_camaras, dataset=True, columns=["id", "descripcion"]
    )
    tiempo = wr.s3.read_parquet(ruta_tiempo, dataset=True, columns=["id", "date"])
    tiempo = tiempo.drop(columns=["year"])
    moneda = wr.s3.read_parquet(
        ruta_moneda, dataset=True, columns=["fecha", "uf", "dolar"]
    )
    dim_comites = wr.s3.read_parquet(
        ruta_dim_comites, dataset=True, columns=["id", "codigo_sap"]
    )
    codigos_sap = wr.s3.read_parquet(
        ruta_codigos_sap, dataset=True, columns=["camara", "id_camara_sap"]
    )

    codigos_sap.rename(columns={"id_camara_sap": "distributionrulecode1"}, inplace=True)
    codigos_sap["camara"] = codigos_sap.camara.str.upper()

    resumen = resumen.merge(codigos_sap, on="distributionrulecode1", how="left")

    tiempo["date"] = pd.to_datetime(tiempo.date)
    moneda["fecha"] = pd.to_datetime(moneda.fecha)
    resumen["postingdate"] = pd.to_datetime(resumen.postingdate)

    resumen.rename(columns={"distributionrulecode1": "codigo_sap"}, inplace=True)
    tiempo.rename(columns={"id": "id_fecha", "date": "postingdate"}, inplace=True)
    camaras.rename(columns={"id": "id_camara", "descripcion": "camara"}, inplace=True)
    cuentas.rename(columns={"codigo": "id_cuenta_sap"}, inplace=True)
    cuentas["accountcode"] = cuentas.id_cuenta_sap
    dim_comites.rename(columns={"id": "id_comite"}, inplace=True)
    moneda.rename(columns={"fecha": "postingdate"}, inplace=True)

    df_merge = resumen.merge(tiempo, on="postingdate", how="left")
    df_merge = df_merge.merge(cuentas, on="accountcode", how="left")
    df_merge = df_merge.merge(camaras, on="camara", how="left")
    df_merge = df_merge.merge(moneda, on="postingdate", how="left")
    df_merge = df_merge.merge(dim_comites, on="codigo_sap", how="left")

    df_merge.fillna(
        {"id_camara": 99999999, "id_cuenta_sap": "99999999", "id_comite": 99999999},
        inplace=True,
    )

    df_merge["debit_lc_uf"] = 0
    df_merge["credit_lc_uf"] = 0
    df_merge["debit_sc_uf"] = 0
    df_merge["credit_sc_uf"] = 0
    df_merge["debit_lc_usd"] = 0
    df_merge["credit_lc_usd"] = 0
    df_merge["debit_sc_usd"] = 0
    df_merge["credit_sc_usd"] = 0

    df_merge["fecha_creacion"] = datetime.now(tz=CLT).strftime("%Y-%m-%d %H:%M:%S")
    df_merge["fecha_actualizacion"] = datetime.now(tz=CLT).strftime("%Y-%m-%d %H:%M:%S")

    df_merge.fecha_creacion = pd.to_datetime(df_merge.fecha_creacion)
    df_merge.fecha_actualizacion = pd.to_datetime(df_merge.fecha_actualizacion)

    df_merge.loc[
        df_merge.projectcode.str.startswith("FR"), "is_project"
    ] = "Fondo Regional"
    df_merge.loc[
        df_merge.projectcode.str.startswith("FN"), "is_project"
    ] = "Proyecto impacto"
    df_merge.loc[
        df_merge.projectcode.str.startswith("FRN"), "is_project"
    ] = "Fondo Regional/Proyecto impacto"

    df_merge.rename(columns={"accountcode": "id"}, inplace=True)
    df = df_merge[
        [
            "id",
            "id_fecha",
            "id_camara",
            "id_cuenta_sap",
            "id_comite",
            "debit_lc",
            "credit_lc",
            "credit_sc",
            "debit_sc",
            "debit_lc_uf",
            "credit_lc_uf",
            "credit_sc_uf",
            "debit_sc_uf",
            "debit_lc_usd",
            "credit_lc_usd",
            "debit_sc_usd",
            "credit_sc_usd",
            "fecha_creacion",
            "fecha_actualizacion",
            "is_project",
            "year",
        ]
    ]

    return wr.s3.to_parquet(
        df=df,
        path=ruta_salida,
        dataset=True,
        mode="overwrite_partitions",
        partition_cols=["year"],
        database="analytics_" + AMBIENTE,
        table="fact_resumen_financiero",
        compression="gzip",
    )


def main():

    app.setupGlue(
        ["ambiente", "uuid", "sns_topic"], ["exec", "full_load", "fecha", "years"]
    )

    years = args.get("years").split(",")
    responses = {}

    for y in years:
        print("🔵   Ejecutando funciones con parámetro year para el año", y)
        responses["fact_resumen_financiero" + y] = fact_resumen_financiero(year=y)
        gc.collect()

    print(json.dumps(responses, indent=2, default=str))

    print("✨ Total", app.getTotalExecutionTime())


if __name__ == "__main__":
    main()
