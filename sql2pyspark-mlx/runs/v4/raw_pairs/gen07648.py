from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .cube("level")
    .agg(
        F.countDistinct("k").alias("distinct_ks"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        F.grouping_id().alias("grouping_id"),
    )
)
