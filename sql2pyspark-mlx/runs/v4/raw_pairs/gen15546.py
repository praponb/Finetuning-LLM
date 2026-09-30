from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "log_ts",
        "k",
        "value",
        F.when(F.col("host").rlike("^(h4|h1)$"), "GROUP_A").otherwise("CORE").alias("host_zone"),
    )
    .orderBy(F.col("log_ts").desc_nulls_last())
)
