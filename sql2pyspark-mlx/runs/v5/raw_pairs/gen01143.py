from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy(F.col("price").asc_nulls_first())
w2 = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_last())
w3 = Window.partitionBy("element")

lines = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

lines_carts = lines.select("qty", "price", "promo", F.explode("coupon_codes").alias("element"))

scored = (
    lines_carts
    .filter(F.col("price").isNull())
    .select(
        "qty",
        "price",
        "promo",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.rank().over(w2).alias("price_rank"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share"),
        F.percent_rank().over(w2).alias("price_percent_rank"),
    )
)

filtered = (
    scored
    .filter((F.col("price") <= 200) & (~F.col("promo").eqNullSafe(F.lit("BUNDLE"))))
    .withColumn("greatest_price_price_share", F.greatest("price", "price_share"))
    .withColumn("qty_plus_price", (F.col("qty") + F.col("price")))
)

base = (
    filtered
    .filter(F.col("promo").eqNullSafe(F.lit("BUNDLE")))
    .select(
        "*",
        (F.col("price_rank") - F.col("price_share")).alias("price_rank_minus_price_share"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("price") >= 263, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_rows"),
        F.sort_array(F.collect_set("promo")).alias("promo_list"),
        F.round(F.sum("price_share") / F.nullif(F.count(F.when(F.col("promo") == "BUNDLE", 1)), F.lit(0)), 2).alias("price_share_per_bundle"),
    )
)
