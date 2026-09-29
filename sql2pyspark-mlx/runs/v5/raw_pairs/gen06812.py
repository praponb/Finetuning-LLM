from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_tier").orderBy(F.col("status_code_p90").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 394, "PREMIUM").otherwise("STANDARD").alias("status_code_tier"),
        "key",
    )
    .agg(
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.percentile_approx("status_code", 0.9).alias("status_code_p90"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .withColumn("status_code_p90_rank", F.rank().over(w))
    .orderBy(F.col("key").desc_nulls_last())
)
