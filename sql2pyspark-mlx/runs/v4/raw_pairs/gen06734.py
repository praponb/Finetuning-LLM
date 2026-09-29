from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("num_context").asc_nulls_last())
w_running = Window.partitionBy("service")
w3 = Window.partitionBy("service").orderBy("num_context", "id").rowsBetween(-5, -1)

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "payload_json",
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.col("context")["trace"].alias("trace"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.percent_rank().over(w).alias("num_context_percent_rank"),
        F.sum("num_context").over(w_running).alias("service_sum_num_context"),
        F.avg("num_context").over(w3).alias("num_context_avg_5_prior"),
    )
)
