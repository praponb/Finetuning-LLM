from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("key")
w_grp = Window.partitionBy("value_2").orderBy(F.col("is_welcome5_filled").asc_nulls_first())
w3 = Window.partitionBy("key", "value_2").orderBy(F.col("is_welcome5").desc_nulls_first())
w_recent = Window.partitionBy("key", "value_2").orderBy("is_welcome5")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

lines_carts = lines.select("key", "value", F.explode_outer("coupon_codes").alias("value_2"))

cleaned = (
    lines_carts
    .filter(F.col("key").isin(["device", "utm_source"]))
    .select(
        "*",
        F.when(F.col("value_2") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

valid = (
    cleaned
    .filter(F.col("value_2").isNotNull())
    .withColumn("key_title", F.initcap(F.trim("key")))
)

valid_carts = (
    valid
    .filter(F.col("value_2").isNotNull() & (~F.col("key").eqNullSafe(F.lit("utm_source"))))
    .select(
        "*",
        F.initcap(F.trim("value_2")).alias("value_2_title"),
        F.coalesce("is_welcome5", F.lit(0)).alias("is_welcome5_filled"),
        F.coalesce("value_2", F.lit("NONE")).alias("resolved_value_2"),
    )
)

metrics = (
    valid_carts
    .select(
        "*",
        F.stddev_samp("is_welcome5_filled").over(w_part).alias("key_stddev_samp_is_welcome5_filled"),
        F.cume_dist().over(w_grp).alias("is_welcome5_filled_cume_dist"),
        F.min("is_welcome5_filled").over(w_part).alias("key_min_is_welcome5_filled"),
        F.cume_dist().over(w3).alias("is_welcome5_cume_dist"),
        F.rank().over(w_recent).alias("is_welcome5_rank"),
    )
)

valid_carts_2 = (
    metrics
    .filter(F.col("key").eqNullSafe(F.lit("gift")))
    .select(
        "key",
        "value",
        "value_2",
        "is_welcome5",
        "key_title",
        "value_2_title",
        "is_welcome5_filled",
        "resolved_value_2",
        "key_stddev_samp_is_welcome5_filled",
        "is_welcome5_filled_cume_dist",
        "is_welcome5_cume_dist",
        "is_welcome5_rank",
        (F.col("is_welcome5_filled") - F.col("is_welcome5_cume_dist")).alias("is_welcome5_filled_minus_is_welcome5_cume_dist"),
    )
)

result = (
    valid_carts_2
    .groupBy("value_2")
    .agg(
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_count"),
    )
)
