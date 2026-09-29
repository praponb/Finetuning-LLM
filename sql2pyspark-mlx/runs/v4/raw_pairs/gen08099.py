from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("service").orderBy("status_code", "id")
w3 = Window.partitionBy("tier").orderBy("status_code", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("host", "tier").orderBy(F.col("status_code").desc(), "id")
w5 = Window.partitionBy("service", "host").orderBy("status_code", "id")

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "status_code",
        "context",
        F.coalesce(F.col("context")["trace"], F.lit("none")).alias("trace_or_default"),
        F.col("context")["version"].alias("version"),
        F.coalesce(F.col("context")["region"], F.lit("direct")).alias("region_or_default"),
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .withColumn("first_known_region_or_default", F.first("region_or_default", ignorenulls=True).over(w))
    .withColumn("next_status_code", F.lead("status_code", 2).over(w2))
    .withColumn("status_code_avg_3", F.avg("status_code").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w5))
)
