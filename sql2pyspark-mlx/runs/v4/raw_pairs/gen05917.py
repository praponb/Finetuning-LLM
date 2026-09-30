from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    exploded
    .groupBy("host")
    .agg(
        F.count("*").alias("num_rows"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .orderBy(F.col("latency_ms_bit_and").asc_nulls_last())
)
