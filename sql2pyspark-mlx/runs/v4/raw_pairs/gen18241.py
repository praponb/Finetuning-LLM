from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key")
w2 = Window.partitionBy("key").orderBy(F.col("is_device").desc_nulls_first())
w3 = Window.orderBy(F.col("key_stddev_samp_is_device").asc_nulls_first())
w4 = Window.partitionBy("key").orderBy("key_avg_is_device")

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

flattened_carts = (
    flattened
    .select("id", "created_at", "key", "v", F.explode("coupon_codes").alias("code"))
)

cleaned = (
    flattened_carts
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 07:00:00"))) & (F.col("created_at") < "2023-11-08"))
    .select(
        "id",
        "key",
        "v",
        F.lower("code").alias("code_lower"),
        F.initcap(F.trim("key")).alias("key_title"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("key") != "gift")
    .select(
        "*",
        F.when(F.col("key").isin(["weight_kg", "gift"]), "Tier 1").otherwise("Other").alias("key_group"),
        F.when(F.col("key") == "device", 1).otherwise(0).alias("is_device"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

metrics = (
    cleaned_carts
    .withColumn("key_stddev_samp_is_device", F.stddev_samp("is_device").over(w))
    .withColumn("key_avg_is_device", F.avg("is_device").over(w))
)

scored = (
    metrics
    .select(
        "*",
        F.rank().over(w2).alias("is_device_rank"),
        F.count("key_avg_is_device").over(w).alias("key_count_key_avg_is_device"),
        F.max("key_stddev_samp_is_device").over(w).alias("key_max_key_stddev_samp_is_device"),
    )
)

scored_carts = (
    scored
    .withColumn("key_stddev_samp_is_device_rank", F.rank().over(w3))
    .withColumn("key_avg_is_device_cume_dist", F.cume_dist().over(w4))
    .withColumn("key_sum_key_max_key_stddev_samp_is_device", F.sum("key_max_key_stddev_samp_is_device").over(w))
)

stats = (
    scored_carts
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
    .filter(F.col("weight_kg_rows") >= 1)
)

result = (
    stats
    .select("key", "weight_kg_rows")
    .orderBy(F.col("weight_kg_rows").desc_nulls_last())
)
