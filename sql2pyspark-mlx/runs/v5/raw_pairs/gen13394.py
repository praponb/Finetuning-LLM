from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.partitionBy("tag").orderBy(F.col("product_id").desc_nulls_first())
w3 = Window.partitionBy("promo").orderBy(F.col("product_id").desc_nulls_last())
w_ordered = Window.partitionBy("tag")
w5 = Window.orderBy(F.col("product_id_share").desc())

lines = df_carts.select("id", "coupon_codes", F.inline("items"))

exploded = lines.select("product_id", "promo", F.explode("coupon_codes").alias("tag"))

scored = (
    exploded
    .select(
        "product_id",
        "tag",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.cume_dist().over(w2).alias("product_id_cume_dist"),
        F.percent_rank().over(w3).alias("product_id_percent_rank"),
    )
)

enriched = (
    scored
    .filter(F.col("tag") != "SPRING10")
    .select(
        "product_id",
        "product_id_share",
        "product_id_cume_dist",
        F.stddev_samp("product_id_percent_rank").over(w_ordered).alias("tag_stddev_samp_product_id_percent_rank"),
        F.min("product_id_percent_rank").over(w_ordered).alias("tag_min_product_id_percent_rank"),
        F.avg("product_id_percent_rank").over(w_ordered).alias("tag_avg_product_id_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("tag_stddev_samp_product_id_percent_rank") >= 0)
    .select(
        "*",
        F.when(F.col("tag_avg_product_id_percent_rank") >= 1, "High").when(F.col("tag_avg_product_id_percent_rank") >= 0, "Medium").otherwise("Low").alias("tag_avg_product_id_percent_rank_band"),
    )
)

scored_carts = valid.select("*", F.percent_rank().over(w5).alias("product_id_share_percent_rank"))

result = (
    scored_carts
    .withColumn("product_id_share_percent_rank_tier", F.when(F.col("product_id_share_percent_rank") >= 0.79, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["product_id", "product_id_share_percent_rank_tier"], ["product_id"], []],
        "product_id",
        "product_id_share_percent_rank_tier",
    )
    .agg(
        F.percentile_approx("tag_stddev_samp_product_id_percent_rank", 0.9).alias("tag_stddev_samp_product_id_percent_rank_p90"),
        F.count("*").alias("event_count"),
        F.percentile_approx("product_id_share_percent_rank", [0.25, 0.5, 0.75]).alias("product_id_share_percent_rank_quartiles"),
        F.count("*").alias("row_count"),
        F.round(F.avg("product_id"), 2).alias("avg_product_id"),
        F.round(F.avg("tag_stddev_samp_product_id_percent_rank"), 2).alias("avg_tag_stddev_samp_product_id_percent_rank"),
    )
)
