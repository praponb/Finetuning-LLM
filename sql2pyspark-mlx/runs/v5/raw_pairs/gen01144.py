from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("k", "value"),
    )
)

result = (
    exploded
    .groupBy(
        F.when(F.col("status_code") >= 397, "HIGH").otherwise("LOW").alias("status_code_tier"),
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        "k",
    )
    .agg(
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.sort_array(F.collect_set("k")).alias("k_list"),
        F.round(F.sum("status_code") / F.nullif(F.count(F.when(F.col("k") == "version", 1)), F.lit(0)), 2).alias("status_code_per_version"),
    )
    .filter(F.col("status_code_per_version") < 1000000)
    .orderBy("status_code_bit_and")
)
