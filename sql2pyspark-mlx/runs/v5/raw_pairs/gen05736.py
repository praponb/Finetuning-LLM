from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("tier").orderBy("status_code", "id")
w2 = Window.partitionBy("tier", "service").orderBy("status_code", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "service",
        "status_code",
        F.coalesce(F.col("context")["region"], F.lit("unknown")).alias("region_or_default"),
        F.col("context")["version"].alias("version"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    flattened_attrs
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w))
    .withColumn("first_known_version", F.first("version", ignorenulls=True).over(w2))
)
