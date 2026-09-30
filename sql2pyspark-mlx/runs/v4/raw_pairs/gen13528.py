from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("tier").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("tier").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w5 = Window.partitionBy("tier").orderBy("latency_ms", "id").rowsBetween(-5, -1)

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "payload_json",
        "context",
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.col("context").getItem("region").alias("region"),
        F.col("context").getItem("version").alias("version"),
    )
)

result = (
    flattened_attrs
    .withColumn("running_latency_ms", F.sum("latency_ms").over(w))
    .withColumn("cumulative_latency_ms", F.sum("latency_ms").over(w2))
    .withColumn("latency_ms_max_2h", F.max("latency_ms").over(w3))
    .withColumn("latency_ms_sum_2h", F.sum("latency_ms").over(w4))
    .withColumn("latency_ms_avg_5_prior", F.avg("latency_ms").over(w5))
)
