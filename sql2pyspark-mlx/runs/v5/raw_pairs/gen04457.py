from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "tier").orderBy("log_ts", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("host", "tier").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("host").orderBy("latency_ms", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("tier").orderBy("latency_ms", "id")

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "context",
        F.col("context").getItem("trace").alias("trace"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["user"]["tier"].alias("tier"),
        F.col("context")["region"].alias("region"),
    )
)

result = (
    unpacked
    .select(
        "id",
        "log_ts",
        "tier",
        F.avg("latency_ms").over(w).alias("latency_ms_avg_3"),
        F.last("tier", ignorenulls=True).over(w2).alias("last_known_tier"),
        F.sum("latency_ms").over(w3).alias("running_latency_ms"),
        F.lead("latency_ms", 2, 0).over(w4).alias("next_latency_ms"),
    )
)
