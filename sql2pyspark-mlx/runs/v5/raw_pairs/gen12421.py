from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("status_code_high").desc_nulls_first())

parsed = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "payload_json",
        F.shiftright("status_code", 2).alias("status_code_high"),
        F.col("context").getItem("version").alias("version"),
        F.coalesce(F.col("context")["version"], F.lit("none")).alias("version_or_default"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    parsed
    .select(
        "id",
        "level",
        "latency_ms",
        "status_code_high",
        "version_or_default",
        "user_tier",
        F.sum("latency_ms").over(w).alias("running_sum_latency_ms"),
        F.min("latency_ms").over(w).alias("running_min_latency_ms"),
        F.max("latency_ms").over(w).alias("running_max_latency_ms"),
        F.percent_rank().over(w2).alias("status_code_high_percent_rank"),
    )
)
