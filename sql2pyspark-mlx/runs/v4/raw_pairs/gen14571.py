from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    exploded
    .groupBy("host")
    .agg(
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.min("log_ts").alias("started_at"),
    )
    .orderBy(F.col("status_code_stddev").desc_nulls_last())
)
