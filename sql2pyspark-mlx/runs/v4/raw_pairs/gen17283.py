from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("attr_key", "attr_value"))
)

result = (
    lines
    .groupBy(F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.count_if(F.col("attr_key") == "device").alias("device_events"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.count(F.when(F.col("attr_key") == "utm_source", 1)).alias("utm_source_count"),
    )
    .orderBy(F.col("utm_source_count").desc_nulls_first())
)
