from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "level",
        "latency_ms",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    lines
    .groupBy("key")
    .agg(
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
        F.percentile_approx("latency_ms", 0.5).alias("latency_ms_p50"),
    )
    .filter(F.col("latency_ms_p50") < 0)
    .orderBy(F.col("latency_ms_bit_and").asc_nulls_first())
)
