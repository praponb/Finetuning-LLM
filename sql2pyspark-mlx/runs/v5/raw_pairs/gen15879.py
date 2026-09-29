from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("host") == "h3")
    .select(
        "id",
        "host",
        "level",
        F.col("context")["version"].alias("version"),
        F.col("context")["trace"].alias("trace"),
        F.get_json_object("payload_json", "$.user.id").alias("user_id"),
    )
)

result = (
    flattened_attrs
    .cube("level")
    .agg(
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("gid"),
    )
)
