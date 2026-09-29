from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "payload_json",
        F.col("context")["version"].alias("version"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .rollup(
        F.date_trunc("HOUR", "log_ts").alias("hour_window"),
        F.when(F.col("status_code") >= 422, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        ((F.unix_timestamp(F.max("log_ts")) - F.unix_timestamp(F.min("log_ts"))) / 60).alias("duration_min"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
    )
    .filter(F.col("duration_min") < 1000000)
    .orderBy(F.col("status_code_tier").desc_nulls_first())
)
