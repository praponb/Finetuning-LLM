from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") != "billing")
    .select(
        "id",
        "service",
        "level",
        "status_code",
        "payload_json",
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        F.size("context").alias("num_context"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["duration"].alias("duration_sec"),
        F.from_json("payload_json", "STRUCT<user: STRUCT<id: INT, tier: STRING>, duration: DOUBLE, retries: INT, tags: ARRAY<STRING>>")["retries"].alias("retry_count"),
    )
)

result = (
    flattened_attrs
    .groupBy(
        F.when(F.col("retry_count") >= 4, "PREMIUM").otherwise("STANDARD").alias("retry_count_tier"),
    )
    .agg(
        F.stddev_samp("status_code").alias("status_code_stddev"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.size(F.collect_set("service")).alias("num_services"),
    )
)
