from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "created_at", F.explode_outer("attributes").alias("k", "value"))
)

result = (
    flattened
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.min("created_at").alias("first_created_at_2"),
        F.count(F.when(F.col("k") == "device", 1)).alias("device_rows"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("k") == "gift", 1)).alias("gift_rows"),
    )
)
