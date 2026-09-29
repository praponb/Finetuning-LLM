from pyspark.sql import functions as F

flattened_attrs = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "payload_json",
        "context",
        F.col("context").getItem("region").alias("region"),
        F.col("context")["trace"].alias("trace"),
    )
)

result = (
    flattened_attrs
    .groupBy("level")
    .agg(
        F.countDistinct("region").alias("distinct_regions"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.size(F.collect_set("level")).alias("num_levels_2"),
    )
    .filter(F.col("cnt") >= 1)
)
