from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.size("context").alias("num_context"),
        F.col("context")["version"].alias("version"),
        F.array_contains(F.map_keys("context"), "region").alias("has_region"),
    )
)

result = (
    unpacked
    .groupBy(
        F.when(F.col("num_context") >= 86, "PREMIUM").otherwise("STANDARD").alias("num_context_tier"),
    )
    .agg(
        F.min("log_ts").alias("first_log_ts"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("num_context") / F.nullif(F.count(F.when(F.col("service") == "billing", 1)), F.lit(0)), 2).alias("num_context_per_billing"),
        F.count_if(F.col("service") == "search").alias("search_events"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
    )
)
