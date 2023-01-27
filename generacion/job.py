from pyspark.sql import SparkSession
from awsglue.context import GlueContext
import boto3
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import year, month, dayofmonth
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import isnull, col, get_json_object, from_json


## @params: [JOB_NAME]
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])


# args = getResolvedOptions(sys.argv, mandatory_params)
# dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table(args["TABLE_NAME"])
# config = table.get_item(Key={'CEN': args["CONFIG_KEY"]}).get('Item')


def delete_s3(url):
    s3 = boto3.resource("s3")

    arrPath = url.split("/")
    the_bucket = f"{arrPath[2]}"
    the_prefix = f"{arrPath[3]}/{arrPath[4]}/"

    bucket = s3.Bucket(the_bucket)
    bucket.objects.filter(Prefix=the_prefix).delete()


def add_partition_fields(df):
    df = df.withColumn("year", year("fecha"))
    df = df.withColumn("month", month("fecha"))
    df = df.withColumn("day", dayofmonth("fecha"))
    return df


spark = (
    SparkSession.builder.appName("generacion_real_full")
    .config(
        "hive.metastore.client.factory.class",
        "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    )
    .enableHiveSupport()
    .getOrCreate()
)


sc = spark.sparkContext
glueContext = GlueContext(sc)
spark.conf.set("spark.sql.crossJoin.enabled", "true")

conn_ops = {
    "paths": ["s3://cen-datalake-staging/opreal/ops_measurement/"],
    "recurse": True,
}

df_ops_measure = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options=conn_ops,
    format="parquet",
    transformation_ctx="ops_measurement",
)

df_ops_measure = df_ops_measure.toDF()
df_topology_key = spark.read.parquet("s3://cen-datalake-staging/opreal/topology_key")
df_aggregations_keycentral = spark.read.parquet(
    "s3://cen-datalake-staging/opreal/aggregations_keycentral"
)
df_topology_time = spark.read.parquet("s3://cen-datalake-staging/opreal/topology_time")
df_topology_keytype = spark.read.parquet(
    "s3://cen-datalake-staging/opreal/topology_keytype"
)
df_topology_keyunit = spark.read.parquet(
    "s3://cen-datalake-staging/opreal/topology_keyunit"
)
df_topology_keynode = spark.read.parquet(
    "s3://cen-datalake-staging/opreal/topology_keynode"
)
df_topology_node = spark.read.parquet("s3://cen-datalake-staging/opreal/topology_node")
df_topology_edge = spark.read.parquet("s3://cen-datalake-staging/opreal/topology_edge")
df_topology_company = spark.read.parquet(
    "s3://cen-datalake-staging/opreal/topology_company"
)
df_topologia_centrales = spark.read.parquet(
    "s3://cen-datalake-staging/infotecnica/topologia_centrales"
)
df_topologia_empresas = spark.read.parquet(
    "s3://cen-datalake-staging/infotecnica/topologia_empresas"
)


### Creacion de Vista para usar SQL

df_ops_measure.createOrReplaceTempView("df_ops_measure")
df_topology_key.createOrReplaceTempView("df_topology_key")
df_aggregations_keycentral.createOrReplaceTempView("df_aggregations_keycentral")
df_topology_time.createOrReplaceTempView("df_topology_time")
df_topology_keytype.createOrReplaceTempView("df_topology_keytype")
df_topology_keyunit.createOrReplaceTempView("df_topology_keyunit")
df_topology_keynode.createOrReplaceTempView("df_topology_keynode")
df_topology_node.createOrReplaceTempView("df_topology_node")
df_topology_edge.createOrReplaceTempView("df_topology_edge")
df_topology_company.createOrReplaceTempView("df_topology_company")
df_topologia_centrales.createOrReplaceTempView("df_topologia_centrales")
df_topologia_empresas.createOrReplaceTempView("df_topologia_empresas")
# , CAST("json_extract"(CAST(tk.params AS varchar), '$.max_tol') AS varchar) "Potencia Maxima"
query = f"""
    WITH
  ops AS (
   SELECT
     *
   , row_number() OVER (PARTITION BY reported_time_id, key_id ORDER BY audit_id DESC) orden
   FROM
     df_ops_measure
) 
    
select 
     om.value as valor 
    , tku.natural_key as unidad
    , tt.date as fecha
    , tt.hour as Hora
    , tk.name as  Nombre_Central_Unidad
    , tu2.description as Tipo_Tecnologia
    , te2.empresanombre as Propietario
    , tk.params as params
    , tk.id as Idcentral
from  ((((((((((((ops om
INNER JOIN df_topology_key tk ON (om.key_id = tk.id))
INNER JOIN df_aggregations_keycentral ak ON (tk.id = ak.key_id))
INNER JOIN df_topology_time tt ON (om.reported_time_id = tt.id))
INNER JOIN df_topology_keytype tkt ON (tkt.id = tk.key_type_id))
INNER JOIN df_topology_keyunit tku ON (tku.id = tkt.key_unit_id))
INNER JOIN df_topology_keynode kn ON (tk.id = kn.key_id))
INNER JOIN df_topology_node tu ON (kn.node_id = tu.id))
INNER JOIN df_topology_edge te ON (tu.id = te.src_id))
INNER JOIN df_topology_node tu2 ON (te.dst_id = tu2.id))
INNER JOIN df_topology_company tc ON (tk.company_id = tc.id))
INNER JOIN df_topologia_centrales tc2 ON (tu.description = tc2.centralnombre))
INNER JOIN df_topologia_empresas te2 ON (tc2.id_propietario = te2.id_empresa))
WHERE ((om.orden = 1) AND ((tk.key_type_id IN (1, 72)) AND (tu2.node_type_id = 1)))

"""

fact_generacion_test = spark.sql(query)
fact_generacion_test = fact_generacion_test.withColumn(
    "potencia_maxima", get_json_object(fact_generacion_test.params, f"$.max_tol")
)
fact_generacion_final = fact_generacion_test.select(
    "valor",
    "unidad",
    "fecha",
    "Hora",
    "Nombre_Central_Unidad",
    "Tipo_Tecnologia",
    "Propietario",
    "potencia_maxima",
    "Idcentral",
)
# fact_generacion_final.show(n=5)

# fact_generacion_final = add_partition_fields(fact_generacion_final)
fact_generacion_final = fact_generacion_final.repartition(20)


Transform0 = DynamicFrame.fromDF(fact_generacion_final, glueContext, "df")

path = "s3://cen-datalake-staging/opreal/generacion_real/"
delete_s3(path)

DataSink0 = glueContext.getSink(
    path=path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["fecha"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="DataSink0",
)


DataSink0.setCatalogInfo(
    catalogDatabase="staging_prd", catalogTableName="generacion_real_table"
)
DataSink0.setFormat("glueparquet")
# glueContext.purge_s3_path(path, {"retentionPeriod": 0})
DataSink0.writeFrame(Transform0)
