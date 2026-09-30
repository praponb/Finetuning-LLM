from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("host")
    .agg(
        F.bit_or("status_code").alias("status_code_bit_or"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.bit_and("status_code").alias("status_code_bit_and"),
        F.sum(F.when(F.col("service") == "auth", F.col("status_code")).otherwise(0)).alias("auth_status_code"),
        F.sum(F.when(F.col("attr_key") == "region", F.col("status_code"))).alias("region_total_status_code"),
    )
    .filter(F.col("debug_rows") > 1)
    .orderBy(F.col("host").desc_nulls_last())
)
