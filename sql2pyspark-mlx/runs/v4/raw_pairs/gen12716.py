from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_context", "id")
w2 = Window.partitionBy("tier").orderBy("num_context", "id")

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        "payload_json",
        "context",
        F.coalesce(F.col("context")["version"], F.lit("direct")).alias("version_or_default"),
        F.size("context").alias("num_context"),
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context").getItem("region"), F.lit("none")).alias("region_or_default"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
    )
)

result = (
    parsed
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("seq_no_2", F.row_number().over(w))
    .withColumn("num_context_bucket", F.ntile(10).over(w2))
)
