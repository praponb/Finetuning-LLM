from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("price").desc())
w_part = Window.partitionBy("promo").orderBy(F.col("coupon_codes_count").asc_nulls_first())
w4 = Window.orderBy("promo_avg_qty")
w5 = Window.orderBy(F.col("product_id_share").desc_nulls_last())

lines = df_carts.select("id", "coupon_codes", F.inline("items"))

filtered = (
    lines
    .filter(F.col("promo").isin(["VIP", "BUNDLE", "WELCOME5"]))
    .select("*", F.size("coupon_codes").alias("coupon_codes_count"))
)

scored = (
    filtered
    .select(
        "id",
        "price",
        "coupon_codes_count",
        F.round((F.col("product_id") / F.sum("product_id").over(w)), 4).alias("product_id_share"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
        F.percent_rank().over(w_part).alias("coupon_codes_count_percent_rank"),
        F.avg("qty").over(w).alias("promo_avg_qty"),
    )
)

kept_keys = (
    scored
    .select("id")
    .exceptAll(
        df_carts
        .filter(F.hour("created_at").between(6, 13))
        .select("id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered_carts = (
    reconciled
    .filter(F.col("price_percent_rank") >= 0.49)
    .select(
        "product_id_share",
        "coupon_codes_count_percent_rank",
        "promo_avg_qty",
        F.greatest("coupon_codes_count_percent_rank", "coupon_codes_count").alias("greatest_coupon_codes_count_percent_rank_coupon_codes_count"),
    )
)

cleaned = (
    filtered_carts
    .filter(F.col("product_id_share") >= 0.6)
    .withColumn("coupon_codes_count_percent_rank_minus_greatest_coupon_codes_count_percent_rank_coupon_codes_count", (F.col("coupon_codes_count_percent_rank") - F.col("greatest_coupon_codes_count_percent_rank_coupon_codes_count")))
    .withColumn("greatest_coupon_codes_count_percent_rank_coupon_codes_count_times_coupon_codes_count_percent_rank", (F.col("greatest_coupon_codes_count_percent_rank_coupon_codes_count") * F.col("coupon_codes_count_percent_rank")))
)

scored_carts = (
    cleaned
    .withColumn("promo_avg_qty_percent_rank", F.percent_rank().over(w4))
    .withColumn("product_id_share_rank", F.rank().over(w5))
)

result = (
    scored_carts
    .select(
        "product_id_share",
        "coupon_codes_count_percent_rank",
        "promo_avg_qty",
        "greatest_coupon_codes_count_percent_rank_coupon_codes_count",
        "greatest_coupon_codes_count_percent_rank_coupon_codes_count_times_coupon_codes_count_percent_rank",
        "promo_avg_qty_percent_rank",
        "product_id_share_rank",
    )
    .orderBy(F.col("product_id_share_rank").asc_nulls_first())
)
