from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("product_id").asc_nulls_last())
w2 = Window.partitionBy("promo", "customer_id")
w3 = Window.partitionBy("qty_share_tier").orderBy(F.col("product_id_per_bundle").desc())

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

cleaned = (
    exploded
    .filter(F.col("customer_id").isNull())
    .withColumn("promo_filled", F.coalesce("promo", F.lit("N/A")))
)

cleaned_carts = (
    cleaned
    .filter(F.col("promo").isNotNull())
    .select("*", F.array_join("coupon_codes", "|").alias("coupon_codes_text"))
)

metrics = (
    cleaned_carts
    .filter(~F.col("promo").isin(["VIP", "SPRING10"]))
    .withColumn("product_id_cume_dist", F.cume_dist().over(w))
)

enriched = metrics.withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w2)), 4))

result = (
    enriched
    .groupBy(
        F.when(F.col("qty_share") >= 0.64, "PREMIUM").otherwise("STANDARD").alias("qty_share_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("product_id", 0.9).alias("product_id_p90"),
        F.round(F.sum("product_id") / F.nullif(F.count(F.when(F.col("promo") == "BUNDLE", 1)), F.lit(0)), 2).alias("product_id_per_bundle"),
    )
    .withColumn("product_id_per_bundle_rank", F.rank().over(w3))
    .orderBy(F.col("product_id_per_bundle").desc_nulls_last())
)
