from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode_outer("attributes").alias("key", "value"))
)

result = (
    lines
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.max("created_at").alias("ended_at"),
        F.size(F.collect_set("key")).alias("num_keys"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("key") == "device", 1)).alias("device_count"),
        F.count_if(F.col("key") == "device").alias("device_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .orderBy("num_rows")
)
