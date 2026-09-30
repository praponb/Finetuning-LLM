from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .groupBy("attr_key", "host")
    .agg(
        F.count(F.when(F.col("attr_key") == "region", 1)).alias("region_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.round(F.avg("status_code"), 2).alias("avg_status_code"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
)
