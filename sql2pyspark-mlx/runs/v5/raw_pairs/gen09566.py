from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("is_device").desc_nulls_first())
w2 = Window.partitionBy("tag", "k").orderBy(F.col("is_device").desc_nulls_last())
w3 = Window.partitionBy("tag")
w_recent = Window.partitionBy("k").orderBy(F.col("is_device").asc_nulls_first())

lines = df_carts.select("id", "coupon_codes", F.explode("attributes").alias("k", "v"))

exploded = lines.select("id", "k", F.explode("coupon_codes").alias("tag"))

filtered = (
    exploded
    .filter(F.col("tag").isNotNull() & (~F.col("k").eqNullSafe(F.lit("gift"))))
    .select(
        "*",
        F.when(F.col("k") == "device", 1).otherwise(0).alias("is_device"),
        F.when(F.col("tag").isin(["VIP", "SPRING10"]), "Group A").otherwise("Other").alias("tag_group"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("is_device_dense_rank"),
        F.dense_rank().over(w2).alias("is_device_dense_rank_2"),
        F.min("is_device").over(w3).alias("tag_min_is_device"),
        F.cume_dist().over(w_recent).alias("is_device_cume_dist"),
    )
)

valid = (
    enriched
    .filter(F.col("is_device_dense_rank_2") < 6)
    .withColumn("is_device_dense_rank_minus_is_device", (F.col("is_device_dense_rank") - F.col("is_device")))
    .withColumn("is_device_dense_rank_filled", F.coalesce("is_device_dense_rank", F.lit(0)))
)

result = (
    valid
    .groupBy(
        F.when(F.col("is_device_dense_rank_2") >= 8, "HIGH").otherwise("LOW").alias("is_device_dense_rank_2_tier"),
        F.when(F.col("is_device_dense_rank_filled") >= 89, "LARGE").otherwise("SMALL").alias("is_device_dense_rank_filled_tier"),
        "k",
    )
    .agg(
        F.round(F.sum("tag_min_is_device") / F.nullif(F.count(F.when(F.col("tag") == "WELCOME5", 1)), F.lit(0)), 2).alias("tag_min_is_device_per_welcome5"),
        F.max("is_device").alias("max_is_device"),
        F.sum(F.when(F.col("k") == "device", F.col("is_device_dense_rank_2")).otherwise(0)).alias("device_is_device_dense_rank_2"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.min("is_device_dense_rank").alias("min_is_device_dense_rank"),
        F.sum(F.when(F.col("tag") == "WELCOME5", F.col("is_device_dense_rank_filled")).otherwise(0)).alias("welcome5_is_device_dense_rank_filled"),
    )
    .filter(F.col("device_is_device_dense_rank_2") > 0)
)
