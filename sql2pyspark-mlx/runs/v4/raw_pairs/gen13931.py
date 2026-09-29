from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .groupBy("service", "host")
    .agg(
        F.countDistinct("level").alias("distinct_levels"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
    )
    .filter(F.col("distinct_levels") < 1)
    .orderBy("host")
)
