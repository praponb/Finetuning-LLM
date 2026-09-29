from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("code").orderBy("qty")
w2 = Window.partitionBy("code")
w3 = Window.partitionBy("code").orderBy(F.col("qty_rank").desc_nulls_last())
w4 = Window.orderBy(F.col("qty_rank_cume_dist").desc_nulls_last())
w5 = Window.orderBy("is_welcome5")
w6 = Window.orderBy("qty_rank")
w7 = Window.orderBy(F.col("qty_rank_cume_dist").asc_nulls_last())
w8 = Window.orderBy(F.col("product_id_share").asc_nulls_last())

exploded = (
    df_carts
    .withColumn("entry", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("entry.product_id"),
        F.col("entry.qty"),
    )
)

exploded_carts = (
    exploded
    .select("id", "product_id", "qty", F.explode_outer("coupon_codes").alias("code"))
)

valid = (
    exploded_carts
    .filter(F.col("code").isin(["SPRING10", "VIP"]))
    .select(
        "*",
        F.when(F.col("code") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
        F.lower("code").alias("code_lower"),
        F.coalesce("code", F.lit("DIRECT")).alias("resolved_code"),
    )
)

metrics = (
    valid
    .filter(F.col("code") == "VIP")
    .select(
        "*",
        F.rank().over(w).alias("qty_rank"),
        F.round((F.col("product_id") / F.sum("product_id").over(w2)), 4).alias("product_id_share"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.stddev_samp("product_id").over(w2).alias("code_stddev_samp_product_id"),
    )
)

scored = (
    metrics
    .select(
        "qty",
        "is_welcome5",
        "code_lower",
        "qty_rank",
        "product_id_share",
        "qty_share",
        F.cume_dist().over(w3).alias("qty_rank_cume_dist"),
    )
)

with_windows = (
    scored
    .select(
        "qty",
        "is_welcome5",
        "qty_rank",
        "qty_share",
        "qty_rank_cume_dist",
        F.rank().over(w4).alias("qty_rank_cume_dist_rank"),
        F.cume_dist().over(w5).alias("is_welcome5_cume_dist"),
        F.rank().over(w6).alias("qty_rank_rank"),
        F.rank().over(w7).alias("qty_rank_cume_dist_rank_2"),
        F.cume_dist().over(w8).alias("product_id_share_cume_dist"),
    )
)

valid_carts = (
    with_windows
    .filter(F.col("qty_rank_cume_dist_rank") <= 6)
    .select(
        "is_welcome5",
        "qty_rank",
        "qty_share",
        "qty_rank_cume_dist",
        "qty_rank_cume_dist_rank",
        "is_welcome5_cume_dist",
        "qty_rank_rank",
        "qty_rank_cume_dist_rank_2",
        F.coalesce("qty_rank_cume_dist_rank", F.lit(0)).alias("qty_rank_cume_dist_rank_filled"),
    )
)

result = (
    valid_carts
    .groupBy(
        F.when(F.col("is_welcome5_cume_dist") >= 0.68, "HIGH").otherwise("LOW").alias("is_welcome5_cume_dist_tier"),
        F.when(F.col("qty_share") >= 0.77, "PREMIUM").otherwise("STANDARD").alias("qty_share_tier"),
        F.when(F.col("qty_rank_cume_dist") >= 0.87, "HIGH").otherwise("LOW").alias("qty_rank_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("is_welcome5"), 2).alias("avg_is_welcome5"),
        F.percentile_approx("qty_rank_cume_dist_rank", 0.5).alias("qty_rank_cume_dist_rank_p50"),
        F.count("*").alias("cnt"),
    )
)
