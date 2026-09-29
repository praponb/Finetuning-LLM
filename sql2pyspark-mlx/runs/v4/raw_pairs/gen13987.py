from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "value"),
    )
)

result = (
    lines
    .withColumn("status_code_tier", F.when(F.col("status_code") >= 410, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["status_code_tier", "attr_key"], ["status_code_tier"], []],
        "status_code_tier",
        "attr_key",
    )
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy("status_code_bit_or")
)
