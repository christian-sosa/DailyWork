import pyspark
import datetime
import sys
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, udf, regexp_replace
from pyspark.sql.functions import current_date
from pyspark.sql.types import *
from dateutil import tz
from dateutil import relativedelta

CLT = tz.gettz("America/Santiago")

spark = SparkSession.builder.appName("app").getOrCreate()

conf = pyspark.SparkConf()
sc = pyspark.SparkContext.getOrCreate(conf=conf)
spark = SparkSession.builder.getOrCreate()
sqlContext = SQLContext(sc)


@udf
def set_uso(persona_lista, numero_cv):
    if persona_lista > 0 or persona_lista < 0:
        return "Si"
    elif numero_cv > 0 or numero_cv < 0:
        return "Si"
    else:
        return "No"


from utiles import (
    capitalizar,
    CCHC_DW_BUCKETS,
    escribirCatalogoWrapper,
    insertarConteoWrapper,
    notificar,
    getArgs,
    homologar,
    convertir_ascii,
    insertarTransaccion,
)

args = getArgs(["uuid", "ambiente", "catalogo"], sc)

ambiente = args.get("ambiente", "local")
job_id = args.get("uuid", 0)
fecha = args.get("fecha")


AMBIENTE = args["ambiente"]
JOB_ID = args["uuid"]
staging = CCHC_DW_BUCKETS[AMBIENTE]["staging"]
analytics = CCHC_DW_BUCKETS[AMBIENTE]["analytics"]
raw = CCHC_DW_BUCKETS[AMBIENTE]["raw"]


def dim_bne_empresas():
    args["tabla"] = "dim_bne_empresas"
    empresas_df = spark.read.parquet("s3://cchc-dw-qa-staging/" + "bne/empresas")
    actividad_emps_df = spark.read.parquet(
        "s3://cchc-dw-qa-staging/" + "bne/actividad_empresas"
    )
    ignorar = spark.read.parquet(
        "s3://cchc-dw-qa-staging/" + "bne/empresas_no_considerar"
    )
    # cvs_vistos = spark.read.parquet(staging + "bne/cv_vistos")
    dim_socios = spark.read.parquet("s3://cchc-dw-qa-analytics/" + "dim_socios")
    nomina_empresas = spark.read.parquet(
        "s3://cchc-dw-qa-staging/" + "sii/nomina_empresas/commercial_year=2021/"
    )
    bne_usuarios_empresas = spark.read.parquet(
        "s3://cchc-dw-dev-raw/" + "bne/bne_usuarios_empresas_v2"
    )

    cantidad_cvs_ae = actividad_emps_df.groupBy("rut").agg(
        f.sum("persona_lista").alias("persona_lista")
    )

    print("actividad empresas")
    print(cantidad_cvs_ae.columns)
    nomina_empresas = nomina_empresas.select(
        "rut",
        "numero_de_trabajadores_dependientes_informados",
        "rubro_economico",
        "tamano_de_empresa",
        "tramo_de_facturacion_txt",
        "tramo_trabajadores",
    )

    nomina_empresas = nomina_empresas.withColumn(
        "rut", f.regexp_replace(f.col("rut"), "\\-", "")
    )
    nomina_empresas = nomina_empresas.withColumn("rut", f.upper(f.col("rut")))

    empresas_df = empresas_df.where("estado = 1")
    ignorar = ignorar.withColumn("ignorar", f.lit(True)).select("rut", "ignorar")

    print("ignorar")
    print(ignorar.columns)
    df = empresas_df.join(ignorar, "rut", "left")

    df = df.join(cantidad_cvs_ae, "rut", "left")

    bne_usuarios_empresas_agg = bne_usuarios_empresas.groupBy("rut").agg(
        f.sum("vistas_cv").alias("cv_vistos"),
        f.sum("descarga_cv").alias("numero_cv_descargados"),
        f.max("fecha_alta").alias("ultima_actividad"),
    )

    bne_usuarios_empresas_agg = bne_usuarios_empresas_agg.withColumn(
        "difference", f.datediff(f.current_date(), f.col("ultima_actividad"))
    ).withColumn("difference", f.col("difference").cast("int"))

    bne_usuarios_empresas_agg = bne_usuarios_empresas_agg.selectExpr(
        "*",
        """
        CASE 
            WHEN (difference < 61) and (difference > 30) THEN 'CASI INACTIVO'
            WHEN difference < 31 THEN 'ACTIVO'
            ELSE 'INACTIVO'
        END as status
    """,
    )

    print("bne_usuarios_empresas_agg")
    print(bne_usuarios_empresas_agg.columns)

    df = df.join(bne_usuarios_empresas_agg, "rut", "left")

    df = df.drop("ultima_actividad", "difference")

    socios_unicos = (
        dim_socios.where("ESTADO = 'ACTIVO'")
        .dropDuplicates(["RUT"])
        .select("RUT")
        .withColumn("socio", f.lit(1))
        .withColumnRenamed("RUT", "rut")
        .withColumn("rut", regexp_replace("rut", "\\-", ""))
    )

    print("socios_unicos")
    print(socios_unicos.columns)

    df = df.join(socios_unicos, "rut", "left")

    df = df.selectExpr(
        "*",
        """
        CASE 
            WHEN (cv_vistos > 0 or numero_cv_descargados > 0 or persona_lista > 0) and estado = 1 and ignorar is null THEN true
            ELSE false
        END as uso
    """,
    )

    df = df.fillna(
        {
            "persona_lista": 0,
            "status": "INACTIVO",
            "numero_cv_descargados": 0,
            "cv_vistos": 0,
            "socio": 0,
            "ignorar": False,
        }
    )

    start_date = (
        datetime.datetime.now(tz=CLT) - relativedelta.relativedelta(years=1)
    ).strftime("%Y-%m-%d")
    print(start_date)

    df = (
        df.withColumn("fechaalta", f.to_date("fechaalta", "dd/MM/yyyy"))
        .withColumn("tope", f.lit(start_date))
        .withColumn("tope", f.col("tope").cast("date"))
    )

    df = df.selectExpr(
        "*", "CASE WHEN fechaalta >= tope THEN true ELSE false END f_365"
    ).drop("tope")

    df = df.withColumn(
        "santiago_region",
        f.when(f.col("nom_region") == "Metropolitana", "Santiago").otherwise(
            "Regiones"
        ),
    )

    df = df.where("ignorar = false")

    geografia = (
        spark.read.parquet("s3://cchc-dw-qa-analytics/" + "geografia_variaciones")
        .withColumn("comuna_ascii", convertir_ascii("comuna"))
        .select("idconecta", "comuna_ascii")
        .withColumnRenamed("idconecta", "id_camara")
        .distinct()
    )

    df = (
        df.withColumn("comuna_ascii", convertir_ascii("nom_comuna"))
        .join(geografia, "comuna_ascii", "left")
        .withColumn(
            "nom_comuna",
            f.when(f.col("comuna_ascii") == "migracion", "Sin Información").otherwise(
                f.col("nom_comuna")
            ),
        )
    )

    print("geografia")
    print(geografia.columns)

    df = df.withColumn("rut", f.upper(f.col("rut")))

    print("nomina_empresas")
    print(nomina_empresas.columns)
    df = df.join(nomina_empresas, "rut", "left")

    print("df")
    print(df.columns)

    df = df.drop("comuna_ascii", "day")

    print("df")
    print(df.columns)
    escribirCatalogoWrapper(df, analytics + args["tabla"], args, sc)
    count = df.count()
    insertarConteoWrapper(count, args)


def bne_trabajadores():
    args["tabla"] = "bne_trabajadores"
    ruta_trabajadores = "s3://cchc-dw-qa-staging/" + "bne/trabajadores/"
    ruta_normalizador_especialidades = (
        "s3://cchc-dw-qa-staging/" + "bne/normalizador_especialidad"
    )
    ruta_normalizador_oficio = "s3://cchc-dw-qa-staging/" + "bne/normalizador_oficio"
    ruta_cesantes = "s3://cchc-dw-qa-staging/" + "bne/cesantes"

    ruta_salida = "s3://cchc-dw-dev-analytics/" + "bne_trabajadores"

    print(
        "leyendo desde "
        + ruta_trabajadores
        + ruta_normalizador_oficio
        + ruta_normalizador_especialidades
        + ruta_normalizador_oficio
        + ruta_cesantes
    )
    dfTrabajadores = spark.read.parquet(ruta_trabajadores)
    dfEspecialidades = spark.read.parquet(ruta_normalizador_especialidades)
    dfOficio = spark.read.parquet(ruta_normalizador_oficio)
    dfCesantes = spark.read.parquet(ruta_cesantes)

    dfTrabajadores.alias("trabajadores")
    dfOficio.alias("oficios")
    dfEspecialidades.alias("especialidades")

    # Proper text
    dfTrabajadores = dfTrabajadores.withColumn("ocupacion", capitalizar("ocupacion"))

    dfTrabajadores.createOrReplaceTempView("trabajadores")
    dfOficio.createOrReplaceTempView("oficios")
    dfEspecialidades.createOrReplaceTempView("especialidades")
    dfCesantes.createOrReplaceTempView("cesantes")

    sql_joins = """
        SELECT
            fecha_registro,
            -- num_documento,
            -- digito_verificador,
            t.ocupacion,
            rango_experiencia,
            region,
            comuna,
            edad,
            sexo,
            oficio,
            especialidad,
            concat(num_documento, digito_verificador) as rut,
            CASE
                WHEN rango_experiencia = 'Sin experiencia' THEN 1
                WHEN rango_experiencia = 'De 0 a 1 año' THEN 2
                WHEN rango_experiencia = 'De 1 a 2 años' THEN 3
                WHEN rango_experiencia = 'De 2 a 5 años' THEN 4
                WHEN rango_experiencia = 'Más de 5 años' THEN 5
                ELSE null
            END as OrdenRangoExperiencia,
            CASE
                WHEN Edad <= 30 THEN 'Menor a 30'
                WHEN Edad > 30 and Edad <= 40 THEN 'Entre 31 y 40'
                WHEN Edad > 40 and Edad <= 50 THEN 'Entre 41 y 50'
                WHEN Edad > 50 and Edad <= 60 THEN 'Entre 51 y 60'
                ELSE 'Mayor a 60'
            END as tramo_de_edad,
            c.fundacion_social,
            CASE
                WHEN Sexo = 'M' THEN 'Mujeres'
                WHEN Sexo = 'H' THEN 'Hombres'
                ELSE 'Sin Información'
            END as genero
        FROM trabajadores t
        LEFT JOIN oficios o ON o.oficio = t.ocupacion
        LEFT JOIN especialidades e ON e.ocupacion = t.ocupacion
        LEFT JOIN cesantes c ON c.rut = concat(num_documento, upper(digito_verificador))
    """
    print("JOIN especialidades, cesantes y oficios")
    df = spark.sql(sql_joins)

    df = df.withColumn("comuna_ascii", convertir_ascii("comuna"))

    df.createOrReplaceTempView("temporal2")

    sql_add_orden = """
        SELECT
            *,
            CASE
                WHEN tramo_de_edad = 'Menor a 30' THEN 1
                WHEN tramo_de_edad = 'Entre 31 y 40' THEN 2
                WHEN tramo_de_edad = 'Entre 41 y 50' THEN 3
                WHEN tramo_de_edad = 'Entre 51 y 60' THEN 4
                WHEN tramo_de_edad = 'Mayor a 60' THEN 5
                ELSE null
            END as OrdenTramoEdad
        FROM temporal2
    """
    print("Tramo de edades")
    df = spark.sql(sql_add_orden)
    print("Cambiar nulos por no en fundacion_social")
    df = df.na.fill("No", subset=["fundacion_social"])

    df = (
        df.withColumn("clave_combinada", f.concat_ws("", df.rut, df.ocupacion))
        .withColumn(
            "experiencia_combinada", f.concat_ws("", df.rut, df.rango_experiencia)
        )
        .withColumn("oficio_combinada", f.concat_ws("", df.rut, df.oficio))
        .withColumn(
            "especialidades_combinada", f.concat_ws("", df.rut, df.especialidad)
        )
    )

    print("castear fecha_registro a date")
    df = df.withColumn("fecha_registro", col("fecha_registro").cast("date"))

    df = df.fillna({"especialidad": "Sin Información"})

    # Agregar cámara a la que corresponde la persona mediante la tabla bne geografía
    geo = spark.read.parquet("s3://cchc-dw-qa-analytics/" + "geografia_variaciones")
    comunas_df = (
        geo.select("idconecta", "comuna")
        .withColumnRenamed("idconecta", "id_camara_regional")
        .distinct()
        .withColumn("comuna_ascii", convertir_ascii("comuna"))
        .drop("comuna")
        .withColumn("id_camara_regional", f.col("id_camara_regional").cast("bigint"))
    )

    df = (
        df.join(comunas_df, "comuna_ascii", "left")
        .drop("comuna_ascii")
        .selectExpr(
            "*",
            """
        CASE WHEN id_camara_regional = 1 THEN 'Santiago'
            WHEN id_camara_regional is not null and id_camara_regional != 1 THEN 'Regiones'
            ELSE 'Sin información'
        END santiago_region
        """,
        )
    )

    df = df.dropDuplicates(["rut", "ocupacion"])

    print("escribiendo")
    escribirCatalogoWrapper(df, ruta_salida, args, sc)

    transaccion = df.agg({"fecha_registro": "max"}).first()[0]
    insertarTransaccion(args, transaccion, "bne_trabajadores")


def bne_registros_empresa():
    args["tabla"] = "bne_registros_empresa"

    historico_empresas_ruta = (
        CCHC_DW_BUCKETS[ambiente]["staging"] + "bne/historico_registros_empresa/"
    )

    dfEmpresas = spark.read.parquet(historico_empresas_ruta)
    print("transformando empresas")
    dfEmpresas = dfEmpresas.withColumnRenamed("no_validad", "NoValidada")
    escribirCatalogoWrapper(dfEmpresas, analytics + args["tabla"], args, sc)


def bne_registros_persona():
    args["tabla"] = "bne_registros_persona"

    historico_personas_ruta = (
        CCHC_DW_BUCKETS[ambiente]["staging"] + "bne/historico_registros_persona/"
    )

    dfPersonas = spark.read.parquet(historico_personas_ruta)
    print("transformando personas")
    dfPersonas = dfPersonas.withColumnRenamed("dia", "Fecha")
    escribirCatalogoWrapper(dfPersonas, analytics + args["tabla"], args, sc)


def bne_cesantes():
    args["tabla"] = "bne_cesantes"

    df = spark.read.parquet(staging + "bne/cesantes")
    escribirCatalogoWrapper(df, analytics + "bne_cesantes", args, sc)


def bne_normalizador_oficio():
    args["tabla"] = "bne_normalizador_oficio"

    df = spark.read.parquet(staging + "bne/normalizador_oficio")
    escribirCatalogoWrapper(df, analytics + "bne_normalizador_oficio", args, sc)


def bne_normalizador_especialidades():
    args["tabla"] = "bne_normalizador_especialidades"

    df = spark.read.parquet(staging + "bne/normalizador_especialidad")
    escribirCatalogoWrapper(df, analytics + "bne_normalizador_especialidades", args, sc)


dim_bne_empresas()
bne_trabajadores()
# bne_cesantes()
# bne_normalizador_especialidades()
# bne_normalizador_oficio()
# bne_registros_empresa()
# bne_registros_persona()
