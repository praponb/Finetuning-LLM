from pyspark.sql import functions as F

unpacked = (
    df_app_logs
    .select(
        "id",
        "host",
        F.coalesce(F.col("context").getItem("version"), F.lit("direct")).alias("version_or_default"),
        F.array_contains(F.map_keys("context"), "version").alias("has_version"),
        F.get_json_object("payload_json", "$.user.tier").alias("user_tier"),
    )
)

result = (
    unpacked
    .groupBy("host")
    .agg(
        F.size(F.collect_set("host")).alias("num_hosts"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_rows"),
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
    )
    .orderBy("h1_rows")
)
