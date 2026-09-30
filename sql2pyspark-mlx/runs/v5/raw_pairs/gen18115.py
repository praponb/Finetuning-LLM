from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .filter(F.col("service") == "checkout")
    .select(
        "id",
        "service",
        "host",
        "level",
        "context",
        F.array_contains(F.map_keys("context"), "trace").alias("has_trace"),
        F.coalesce(F.col("context")["region"], F.lit("none")).alias("region_or_default"),
    )
)

result = (
    flattened_attrs
    .rollup("level", "service")
    .agg(
        F.count_if(F.col("service") == "checkout").alias("checkout_events"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.countDistinct("level").alias("distinct_levels"),
        F.count("*").alias("num_rows"),
        F.sort_array(F.collect_set("service")).alias("service_list"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_levels") < 1000)
    .orderBy(F.col("level").asc_nulls_first())
)
