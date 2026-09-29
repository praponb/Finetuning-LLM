from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("pos").desc())
w2 = Window.partitionBy("promo").orderBy(F.col("pos").desc())
w3 = Window.partitionBy("promo").orderBy(F.col("pos").asc_nulls_first())
w_recent = Window.partitionBy("promo")
w5 = Window.partitionBy("promo").orderBy(F.col("pos").desc_nulls_last())
w6 = Window.partitionBy("promo").orderBy("promo_sum_pos")
w7 = Window.partitionBy("promo").orderBy(F.col("pos_dense_rank").asc_nulls_first())
w8 = Window.partitionBy("promo").orderBy(F.col("promo_sum_pos_dense_rank").desc_nulls_last())

flattened = (
    df_carts
    .select("*", F.posexplode("items").alias("pos", "elem"))
    .select(
        "id",
        "coupon_codes",
        "pos",
        F.col("elem.product_id"),
        F.col("elem.qty"),
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

lines = flattened.select("id", "pos", "promo", F.explode("coupon_codes").alias("element"))

enriched = (
    lines
    .withColumn("pos_percent_rank", F.percent_rank().over(w))
    .withColumn("pos_dense_rank", F.dense_rank().over(w2))
    .withColumn("pos_percent_rank_2", F.percent_rank().over(w3))
    .withColumn("promo_sum_pos", F.sum("pos").over(w_recent))
)

matched_keys = (
    enriched
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id") == 2)
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_carts = (
    kept_rows
    .withColumn("pos_cume_dist", F.cume_dist().over(w5))
    .withColumn("promo_max_pos_percent_rank", F.max("pos_percent_rank").over(w_recent))
    .withColumn("promo_sum_pos_dense_rank", F.dense_rank().over(w6))
)

with_windows = (
    enriched_carts
    .select(
        "*",
        F.dense_rank().over(w7).alias("pos_dense_rank_dense_rank"),
        F.percent_rank().over(w8).alias("promo_sum_pos_dense_rank_percent_rank"),
    )
)

result = (
    with_windows
    .withColumn("promo_sum_pos_dense_rank_tier", F.when(F.col("promo_sum_pos_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD"))
    .rollup("promo_sum_pos_dense_rank_tier", "element")
    .agg(
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
        F.max(F.when(F.col("promo") == "BUNDLE", F.col("promo_max_pos_percent_rank"))).alias("bundle_max_promo_max_pos_percent_rank"),
        F.percentile_approx("pos_dense_rank_dense_rank", 0.95).alias("pos_dense_rank_dense_rank_p95"),
        F.grouping("promo_sum_pos_dense_rank_tier").alias("grouping_level"),
    )
    .filter(F.col("vip_rows") >= 1)
    .orderBy(F.col("vip_rows").desc_nulls_first())
)
