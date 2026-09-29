from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .groupingSets(
        [["host", "level", "service"], ["host", "level"], ["host"]],
        "host",
        "level",
        "service",
    )
    .agg(
        F.count(F.when(F.col("host") == "h3", 1)).alias("h3_count"),
        F.corr("status_code", "latency_ms").alias("corr_status_code_latency_ms"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.sort_array(F.collect_set("attr_key")).alias("attr_key_list"),
        F.countDistinct("host").alias("distinct_hosts"),
        F.min("status_code").alias("min_status_code"),
        F.grouping("host").alias("grouping_level"),
    )
    .orderBy(F.col("service").desc())
)
