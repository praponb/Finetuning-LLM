from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

base = (
    flattened
    .filter(~F.col("host").isin(["h3", "h1", "h4"]))
    .select(
        "id",
        "service",
        "host",
        "level",
        "attr_key",
        F.initcap(F.trim("service")).alias("service_title"),
        F.coalesce("attr_key", F.lit("unassigned")).alias("attr_key_filled"),
        F.coalesce("attr_key", F.lit("OTHER")).alias("resolved_attr_key"),
    )
)

filtered = (
    base
    .filter(F.col("attr_key").isNotNull())
    .select(
        "id",
        "service",
        "host",
        "level",
        "service_title",
        "attr_key_filled",
        F.concat_ws("/", "service", "level").alias("service_level_key"),
    )
)

cleaned = (
    filtered
    .filter(F.col("level").isin(["INFO", "ERROR", "DEBUG"]))
    .select(
        "service",
        "host",
        "level",
        "attr_key_filled",
        "service_level_key",
        F.trim("host").alias("clean_host"),
    )
)

result = (
    cleaned
    .groupBy("service", "host")
    .agg(
        F.count("*").alias("cnt"),
        F.count_if(F.col("host") == "h4").alias("h4_events"),
        F.count(F.when(F.col("level") == "INFO", 1)).alias("info_count"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
    )
)
