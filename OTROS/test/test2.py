import pandas as pd
import awswrangler as wr
import numpy as np
from datetime import datetime
from dateutil import tz
from dateutil.relativedelta import relativedelta
from cchc_lib import CChCWrapper, JobStage, JobTypes, Step
import hashlib
import boto3
import unicodedata
import json

app = CChCWrapper(
    job_type=JobTypes.GLUEJOB,
    jobname="conecta_staging",
    stage=JobStage.STAGING,
    arguments=["ambiente", "sns_topic"],
    optional_args=["full_load", "exec"]
)

args = app.getArgs()

AMBIENTE = args["ambiente"]

RAW = f"s3://cchc-dw-{ AMBIENTE }-raw/"
STAGING = f"s3://cchc-dw-{ AMBIENTE }-staging/"
DATABASE = "staging_" + AMBIENTE

fecha = args["fecha"]
day = str(fecha.day).zfill(2)
month = str(fecha.month).zfill(2)
year = fecha.year

partition_key = f"/year={year}/month={month}/"


def get_partitions(which: str) -> list:
    client = boto3.client("s3")

    response = client.list_objects(Bucket=f"cchc-dw-{AMBIENTE}-raw", Prefix=f"conecta/{which}/")

    p_keys = []
    for k in response["Contents"]:
        p_keys.append(k["Key"].split("/")[2:-1])

    return p_keys


def hashear_campo(campo):
    if campo is None:
        return None
    campo = str(campo)
    hash_object = hashlib.sha256(campo.encode())
    hashed = hash_object.hexdigest()

    return hashed


def filtrar_primer_nombre(campo_nombre):
    if campo_nombre is None:
        return None

    campo_nombre = (
        unicodedata.normalize("NFKD", campo_nombre)
        .encode("ascii", "ignore")
        .decode("UTF-8")
        .lower()
    )

    return campo_nombre.split(" ")[0].lower()

@app.notify
@app.perfme
def socios():
    df = wr.s3.read_parquet(RAW + "conecta/socios" + partition_key)
    df.columns = df.columns.str.lower()

    df = df.rename(
        columns={
            "idestado_socio": "id_estado_socio",
            "idtipo_socio": "id_tipo_socio",
            "per_id": "idpersona",
        }
    )

    df["estado"] = df["estado"].str.upper()
    df.loc[df["actividades"].str.contains("Entidad"), "is_entidad"] = "SI"

    df = df.fillna({"comite_principal": "Sin Comité Principal", "entidad": "NO"})

    df.loc[
        df.camara.isin(["ARICA", "ANTOFAGASTA", "CALAMA", "IQUIQUE"]),
        "zona",
    ] = "Zona Norte"

    df.loc[
        df.camara.isin(["COPIAPÓ", "RANCAGUA", "LA SERENA", "VALPARAÍSO"]), "zona"
    ] = "Zona Centro"

    df.loc[
        df.camara.isin(["CONCEPCIÓN", "LOS ÁNGELES", "TEMUCO", "TALCA", "CHILLÁN"]),
        "zona",
    ] = "Zona Sur"

    df.loc[
        df.camara.isin(
            ["VALDIVIA", "OSORNO", "COYHAIQUE", "PUERTO MONTT", "PUNTA ARENAS"]
        ),
        "zona",
    ] = "Zona Austral"

    df.loc[df.camara == "SANTIAGO", "zona"] = "RM"

    df["rut_hash"] = df.rut.apply(lambda x: hashear_campo(x))

    df["cuotas"] = df["cuotas"].astype("Int32")
    df["id_comite_principal"] = df["id_comite_principal"].astype("Int64")

    wr.s3.to_parquet(
        df=df,
        path=STAGING + "conecta/socios",
        dataset=True,
        mode="overwrite",
        database=DATABASE,
        table="conecta_socios",
        compression="gzip",
    )

@app.notify
@app.perfme
def roles():
    df = wr.s3.read_parquet(RAW + "conecta/roles" + partition_key)
    df.columns = df.columns.str.lower()

    df["rut_hash"] = df.rut.apply(lambda x: hashear_campo(x))
    df["numero_socio"] = df.numero_socio.astype("Int64")
    df["id_relacion"] = df.id_relacion.astype("Int64")

    df["fecha_relacion"] = pd.to_datetime(
        df.fecha_relacion, format="%Y-%m-%d"
    ).dt.normalize()

    wr.s3.to_parquet(
        df=df,
        path=STAGING + "conecta/roles",
        table="conecta_roles",
        database=DATABASE,
        dataset=True,
        mode="overwrite",
        dtype={"fecha_relacion": "date"},
        compression="gzip",
    )

@app.notify
@app.perfme
def socios_periodo():
    df = wr.s3.read_parquet(RAW + "conecta/socios_periodo" + partition_key)
    df.columns = df.columns.str.lower()

    df = df.rename(
        columns={
            "idestado_socio": "id_estado_socio",
            "idtipo_socio": "id_tipo_socio",
            "per_id": "idpersona",
            "cuota": "cuotas",
        }
    )

    df["estado"] = df["estado"].str.upper()
    df.loc[df["actividades"].str.contains("Entidad"), "is_entidad"] = "SI"

    df = df.fillna({"comite_principal": "Sin Comité Principal", "entidad": "NO"})

    df.loc[
        df.camara.isin(["ARICA", "ANTOFAGASTA", "CALAMA", "IQUIQUE"]),
        "zona",
    ] = "Zona Norte"

    df.loc[
        df.camara.isin(["COPIAPÓ", "RANCAGUA", "LA SERENA", "VALPARAÍSO"]), "zona"
    ] = "Zona Centro"

    df.loc[
        df.camara.isin(["CONCEPCIÓN", "LOS ÁNGELES", "TEMUCO", "TALCA", "CHILLÁN"]),
        "zona",
    ] = "Zona Sur"

    df.loc[
        df.camara.isin(
            ["VALDIVIA", "OSORNO", "COYHAIQUE", "PUERTO MONTT", "PUNTA ARENAS"]
        ),
        "zona",
    ] = "Zona Austral"

    df.loc[df.camara == "SANTIAGO", "zona"] = "RM"

    df["rut_hash"] = df.rut.apply(lambda x: hashear_campo(x))

    df["cuotas"] = df["cuotas"].astype("Int32")
    df["id_comite_principal"] = df["id_comite_principal"].astype("Int64")

    wr.s3.to_parquet(
        df=df,
        path=STAGING + "conecta/socios_periodo",
        table="conecta_socios_periodo",
        database=DATABASE,
        dataset=True,
        mode="overwrite",
        compression="gzip",
    )

@app.notify
@app.perfme
def grupos():
    df = wr.s3.read_parquet(RAW + "conecta/grupos" + partition_key)

    df.columns = df.columns.str.lower()

    df.loc[df.descripcion.str.len() < 90, "is_descripcion_corta"] = True
    df["largo_descripcion"] = df.descripcion.str.len()
    df = df.fillna({"is_descripcion_corta": False})

    wr_response = wr.s3.to_parquet(
        df=df,
        path=STAGING + "conecta/grupos/",
        table="conecta_grupos",
        database=DATABASE,
        dataset=True,
        mode="overwrite",
        partition_cols=["estado"],
        compression="gzip",
    )

    return {
        "quantity" : len(df),
        "wr_response" : wr_response
    }

@app.notify
@app.perfme
def ficha_personas():
    df = wr.s3.read_parquet(RAW + "conecta/ficha_personas" + partition_key)
    integrantes = wr.s3.read_parquet(STAGING + "conecta/integrantes_gt/", dataset=True)
    roles = wr.s3.read_parquet(STAGING + "conecta/roles/")
    mapa_genero = wr.s3.read_parquet(STAGING + "mapa_genero/", dataset=True)
    
    integrantes.columns = integrantes.columns.str.lower()
    df.columns = df.columns.str.lower()
    mapa_genero.columns = mapa_genero.columns.str.lower()

    df["fecha_creacion"] = pd.to_datetime(
        df.fecha_creacion, format="%Y-%m-%d"
    ).dt.normalize()
    df["fecha_nacimiento"] = pd.to_datetime(
        df.fecha_nacimiento, format="%Y-%m-%d", errors="coerce"
    )

    df.loc[~df.fecha_defuncion.isnull(), "is_difunto"] = "SI"
    df.loc[df.fecha_defuncion.isnull(), "is_difunto"] = "NO"

    integrantes = integrantes[
        (integrantes.estado_participacion == "ACTIVA")
        & (integrantes.estado_grupo == "ACTIVA")
        & (integrantes.nombre_grupo == "Consejo Regional")
    ]

    consejeros = integrantes[["per_id"]].drop_duplicates().copy()
    consejeros["per_id"] = consejeros.per_id.astype("Int64")
    consejeros["is_consejero_regional"] = True

    roles_consejeros = roles[
        (roles.id_rol != 75)
        & (roles.estado_relacion == "ACTIVA")
        & (roles.estado_socio == "Activo")
    ][["per_id"]]
    roles_consejeros["_not_75"] = True
    roles_consejeros = roles_consejeros.drop_duplicates()

    df = (
        df.merge(roles_consejeros, on="per_id", how="left")
        .merge(consejeros, on="per_id", how="left")
        .fillna({"_not_75": False})
    )

    df.loc[
        (df._not_75 == True) & (df.is_consejero_regional == True), "is_consejero"
    ] = True
    df = (
        df.drop(columns=["_not_75", "is_consejero_regional"])
        .rename(columns={"is_consejero": "is_consejero_regional"})
        .fillna({"is_consejero_regional": False})
    )

    df["primer_nombre"] = df.nombre.apply(lambda x: filtrar_primer_nombre(x))


    mapa_genero = mapa_genero[["nombre", "sexo_inferido"]].rename(
        columns={"nombre": "primer_nombre"}
    ).drop_duplicates()

    df = df.merge(mapa_genero, how="left", on="primer_nombre")


    personal_camara = roles.copy()[(roles.id_rol == 75) | (roles.id_rol == 112)][
        ["per_id"]
    ]
    personal_camara = personal_camara.drop_duplicates()
    personal_camara["is_personal_camara"] = True


    relacion_socio = roles.copy()[
        (roles.estado_relacion == "ACTIVA") & (roles.estado_socio == "Activo")
    ][["per_id"]]
    relacion_socio = relacion_socio.drop_duplicates()
    relacion_socio["is_relacionado_socio"] = True


    df = df.merge(personal_camara, how="left", on="per_id")
    df = df.merge(relacion_socio, how="left", on="per_id")

    df.fecha_defuncion = pd.to_datetime(df.fecha_defuncion)
    df.fecha_creacion = pd.to_datetime(df.fecha_creacion)
    df.fecha_nacimiento = pd.to_datetime(df.fecha_nacimiento)


    df["antiguedad_camara"] = (fecha.date() - df.fecha_creacion.dt.date).dt.days / 365
    df.antiguedad_camara = df.antiguedad_camara.round(0)
    df.antiguedad_camara = df.antiguedad_camara.astype("Int32")


    df["edad"] = (fecha.date() - df.fecha_nacimiento.dt.date).dt.days / 365
    df.edad = df.edad.round(0)
    df.edad = df.edad.astype("Int32")

    df.loc[(df.edad > 120) | (df.edad < 18), "is_fecha_nacimiento_valida"] = False
    df = df.fillna({
        "is_fecha_nacimiento_valida": True, 
        "is_consejero_regional": False,
        "is_relacionado_socio" : False,
        "is_personal_camara" : False
    })


    df["nombre_completo"] = df.nombre.fillna("") + " " + df.apellido_paterno.fillna("") + " " +  df.apellido_materno.fillna("")
    df["nombre_completo"] = df.nombre_completo.str.replace(" +", " ", regex=True)

    df = df.drop(columns=["primer_nombre"])

    wr.s3.to_parquet(
        df=df,
        path=STAGING + 'conecta/ficha_personas/',
        dataset=True,
        partition_cols=["estado"],
        mode="overwrite",
        database=DATABASE,
        table="conecta_ficha_personas"
    )

@app.notify
@app.perfme
def integrantes_gt(version: int):

    igt_name = "integrantes_gt"
    if version == 2:
        igt_name += "_v2"

    df = wr.s3.read_parquet(RAW + f"conecta/{ igt_name }" + partition_key)

    df.columns = df.columns.str.lower()

    df["socio_nombre"] = (
        df.per_nom_0.fillna("")
        + " "
        + df.per_ape_pat_1.fillna("")
        + " "
        + df.per_ape_mat_15.fillna("")
    )

    df.per_rut_3 = df.per_rut_3.astype("Int32")
    df.per_rut_3 = df.per_rut_3.astype("string")
    df["rut"] = df.per_rut_3 + "-" + df.per_dv_4
    df["persona_nombre"] = (
        df.per_nom_11.fillna("")
        + " "
        + df.per_ape_pat_12.fillna("")
        + " "
        + df.per_ape_mat_15.fillna("")
    )
    df.per_rut_16 = df.per_rut_16.astype("Int32")
    df.per_rut_16 = df.per_rut_16.astype("string")
    df["persona_rut"] = df.per_rut_16 + "-" + df.per_dv_17
    df["organo_gremial"] = df.gru_nom


    df.rename(
        columns={
            "gru_nom": "nombre_grupo",
            "per_det_raz_soc_5": "razon_social",
            "pre_reg_nro_7": "nro_registro",
            "pre_reg_cuo_8": "cuota",
            "per_tipo_9": "tipo_persona",
            "car_gre_nom_18": "cargo_gremial",
            "prs_fec_nac_23": "persona_fecha_nacimiento",
            "dir_calle_24": "direccion_calle",
            "dir_nro_25": "direccion_numero",
            "sclr_29": "telefono_1",
            "sclr_30": "telefono_2",
            "sclr_31": "correo",
        },
        inplace=True,
    )


    df.tipo_persona = df.tipo_persona.astype("Int32")
    df.nro_registro = df.nro_registro.astype("Int64")
    df["id_socio"] = df.nro_registro


    df["direccion"] = (
        df.direccion_calle.fillna("") + " " + df.direccion_numero.fillna("")
    )

    df.socio_nombre = df.socio_nombre.str.replace(" +", " ", regex=True)
    df.persona_nombre = df.persona_nombre.str.replace(" +", " ", regex=True)
    df.direccion = df.direccion.str.replace(" +", " ", regex=True)

    wr.s3.to_parquet(
        df=df,
        path=STAGING + f"conecta/{igt_name}/",
        table=f"conecta_{igt_name}",
        database=DATABASE,
        dataset=True,
        partition_cols=["estado_participacion"],
        mode="overwrite",
        compression="gzip",
    )

@app.notify
@app.perfme
def cargos():
    df = wr.s3.read_parquet(RAW + "conecta/cargos_personas" + partition_key)
    altos_cargos = wr.s3.read_csv(
        RAW + "otras_fuentes/cargos_ipn/cargos.csv", sep=";"
    )
    df = wr.catalog.sanitize_dataframe_columns_names(df)
    altos_cargos = altos_cargos[ altos_cargos.Cargo_Directivo.notnull()]
    altos_cargos["es_alto_cargo"] = True
    altos_cargos["idcargo"] = altos_cargos.IdCargo.astype("Int64")
    df = df.merge(altos_cargos[["idcargo", "es_alto_cargo"]], how="left", on="idcargo")

    wr.s3.to_parquet(
        df=df,
        path=STAGING + "conecta/cargos_personas/",
        table=f"conecta_cargos_personas",
        database=DATABASE,
        dataset=True,
        mode="overwrite",
        compression="gzip",
    )

@app.notify
@app.perfme
def contactabilidad():
    df = wr.s3.read_parquet(RAW + "conecta/contactabilidad" + partition_key)

    wr.s3.to_parquet(
        df=df,
        path=STAGING + "conecta/contactabilidad/",
        table=f"conecta_contactabilidad",
        database=DATABASE,
        dataset=True,
        mode="overwrite",
        compression="gzip",
    )


def exec_reporte_asistencia(year):
    print("Ejecutando reporta asistencia para las fechas ", year)
    df = wr.s3.read_parquet(
        RAW + f"conecta/reporte_asistencia/year={year}/", dataset=True
    )

    print("Comparando reuniones duplicadas, cantidad previa:", len(df))
    duplicated = df.groupby(["reuid"], as_index=False)[["reufecini"]].nunique()

    duplicated = duplicated.query("reufecini > 1")
    duplicated_data = df[ df.reuid.isin(duplicated.reuid.to_list()) ].groupby(["reuid"], as_index=False)["reufecini"].min()

    duplicated_data = duplicated_data[["reuid", "reufecini"]]
    duplicated_data["to_drop"] = True

    df = df.merge(duplicated_data, "left", ["reuid", "reufecini"])
    df = df[df.to_drop.isna()].drop(columns=["to_drop"])
    print("Reuniones duplicadas eliminadas, cantidad resultante:", len(df))

    palabras_reservadas = [
        "cancelada",
        "eliminada",
        "pospuesta",
        "reagendada",
        "suspendida",
        "postergada", "cancelado",
        "eliminado",
        "pospuesto",
        "reagendado",
        "suspendido",
        "postergado",
    ]

    df.asiasis = df.asiasis.astype("Int32")
    df.loc[df.asiasis == 1, "str_asistencia"] = "SI"
    df["periodo"] = df.year.astype("string") + df.month.astype("string")

    # Agregar una columna con todo en minúsculas para encontrar las palabras reservadas, luego eliminarla
    df["temp_name"] = df.reunom.str.lower()

    df.loc[
        df.temp_name.str.contains("|".join(palabras_reservadas)), "is_nombre_valido"
    ] = False
    df = df.drop(columns=["temp_name"])

    df.sexo = df.sexo.astype("float64")
    df.sexo = df.sexo.astype("Int32")

    df = df.fillna({"str_asistencia": "NO", "is_nombre_valido": True})

    wr_response = wr.s3.to_parquet(
        df=df,
        path=STAGING + f"conecta/sp_reporte_asistencia/",
        table=f"conecta_sp_reporte_asistencia",
        database=DATABASE,
        dataset=True,
        mode="overwrite_partitions",
        compression="gzip",
        partition_cols=["year", "month"],
        dtype={"reufecini": "date", "perfeccrn": "date", "fechatermino": "date"},
    )

    return {"quantity": len(df), "wr_response": wr_response}

@app.notify
@app.perfme
def reporte_asistencia():
    partitions = get_partitions("reporte_asistencia")

    partitions = [
            [x[0].split("=")[1], x[1].split("=")[1]] for x in partitions
        ]  # Convierte el formato ["year=2022", "month=05"] a ["2022", "05"] para luego iterar la función siguiente

    currently_executed = []

    if args.get("full_load") == "true":
        for p in partitions:
            if p[0] not in currently_executed:
                print("Ejecutando fecha", p)
                exec_reporte_asistencia(year=p[0])
                currently_executed.append(p[0])
            
    else:
        if args["fecha"].month <= 2:
            date_start = args["fecha"] - relativedelta(months=3)
        else:
            date_start = args["fecha"]
        
        current_p = date_start
        
        while current_p.date() <= args["fecha"].date():

            _year = str(current_p.year)
            _month = str(current_p.month).zfill(2)

            if [str(_year), _month] in partitions:
                
                if _year not in currently_executed:
                    print("Ejecutando fecha", current_p)
                    exec_reporte_asistencia(year=_year)
                    currently_executed.append(_year)

                current_p += relativedelta(months=1)
            
            else:
                print("No existen datos en la capa de raw para la fecha indicada", _year, _month)


@app.notify
@app.perfme
def reporte_asistencia_actualizado():
    # Este reporte se actualiza todos los días, por ende se procesan 12 meses
    date_start = fecha - relativedelta(months=12)
    print("Procesando desde ", date_start.date())

    partitions = get_partitions("reporte_asistencia_actualizado")

    partitions = [
        [x[0].split("=")[1], x[1].split("=")[1]] for x in partitions
    ]  # Convierte el formato ["year=2022", "month=05"] a ["2022", "05"] para luego iterar la función siguiente

    palabras_reservadas = [
        "cancelada",
        "eliminada",
        "pospuesta",
        "reagendada",
        "suspendida",
        "postergada",
        "cancelado",
        "eliminado",
        "pospuesto",
        "reagendado",
        "suspendido",
        "postergado",
    ]

    print("Procesando particiones", partitions)

    for p in partitions:
        year = p[0]
        month = p[1]

        #Solamente saltar particiones en el caso de que el parámetro --full_load no se encuentre
        #En el caso de que el parámetro se encuentre, se procesan todas las particiones
        if "full_load" not in args or args.get("full_load") != "true":
            if datetime(int(year), int(month), 1).date() < date_start.date():
                print(f"skipping {year}-{month}")
                continue

        df = wr.s3.read_parquet(
            RAW + f"conecta/reporte_asistencia_actualizado/year={year}/month={month}/",
            dataset=True,
        )

        df.asiasis = df.asiasis.astype("Int32")
        df.loc[df.asiasis == 1, "str_asistencia"] = "SI"
        df["periodo"] = df.year.astype("string") + df.month.astype("string")

        # Agregar una columna con todo en minúsculas para encontrar las palabras reservadas, luego eliminarla
        df["temp_name"] = df.reunom.str.lower()

        df.loc[
            df.temp_name.str.contains("|".join(palabras_reservadas)), "is_nombre_valido"
        ] = False
        df = df.drop(columns=["temp_name"])

        df.sexo = df.sexo.astype("float64")
        df.sexo = df.sexo.astype("Int32")

        df = df.fillna({"str_asistencia": "NO", "is_nombre_valido": True})

        wr_response = wr.s3.to_parquet(
            df=df,
            path=STAGING + f"conecta/sp_reporte_asistencia_actualizado/",
            table=f"conecta_sp_reporte_asistencia_actualizado",
            database=DATABASE,
            dataset=True,
            mode="overwrite_partitions",
            compression="gzip",
            partition_cols=["year", "month"],
            dtype={"reufecini": "date", "perfeccrn": "date", "fechatermino": "date"},
        )

        print("ok para la fecha", year, month, wr_response)

@app.notify
@app.perfme
def afiliaciones():
    df = wr.s3.read_parquet(RAW + 'conecta/afiliaciones' + partition_key)
    df.columns = df.columns.str.lower()

    wr.s3.to_parquet(
        df=df,
        path= STAGING + "conecta/afiliaciones/",
        dataset=True,
        mode="overwrite",
        database="staging_" + AMBIENTE,
        table="conecta_afiliaciones",
        compression="gzip"
    )

@app.notify
@app.perfme
def solicitudes():
    df = wr.s3.read_parquet(RAW + 'conecta/solicitudes' + partition_key)

    df.columns = df.columns.str.lower()

    df.fecha = pd.to_datetime(df.fecha)
    df.fecha = df.fecha.dt.date

    df.fecha_resolucion = pd.to_datetime(df.fecha_resolucion)
    df.fecha_resolucion = df.fecha_resolucion.dt.date

    df.numero_socio = df.numero_socio.astype("Int64")

    wr.s3.to_parquet(
        df=df,
        path= STAGING + "conecta/solicitudes/",
        dataset=True,
        mode="overwrite",
        database="staging_" + AMBIENTE,
        table="conecta_solicitudes",
        compression="gzip"
    )

_executables = [
    Step(socios),
    Step(roles),
    Step(socios_periodo),
    Step(grupos),
    Step(integrantes_gt, 1),
    Step(integrantes_gt, 2),
    Step(cargos),
    Step(reporte_asistencia),
    Step(contactabilidad),
    Step(ficha_personas),
    Step(reporte_asistencia_actualizado),
    Step(afiliaciones),
    Step(contactabilidad)
]

exec_list = []
if "exec" in args:
    exec_list = args["exec"].split(",")


print("Parámetros ejecución:")
print(json.dumps(args, indent=2, default=str))

def main():
    app.setSteps(*_executables)
    print(app.runSteps(*exec_list))
    results = app.getPerformanceStats()
    print(pd.DataFrame(results))

if __name__ == "__main__":
    main()