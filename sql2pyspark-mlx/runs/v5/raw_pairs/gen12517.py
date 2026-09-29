from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "payload_json",
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
        F.size("context").alias("num_context"),
        F.get_json_object("payload_json", "$.duration").alias("duration_text"),
        (F.col("status_code").bitwiseAND(1) != 0).cast("int").alias("flag_bit1"),
    )
)

result = (
    unpacked
    .groupBy("level")
    .agg(
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.max(F.when(F.col("level") == "DEBUG", F.col("num_context"))).alias("debug_max_num_context"),
    )
)
