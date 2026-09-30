from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "v"),
    )
)

result = (
    lines
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.bit_or("latency_ms").alias("latency_ms_bit_or"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
)
