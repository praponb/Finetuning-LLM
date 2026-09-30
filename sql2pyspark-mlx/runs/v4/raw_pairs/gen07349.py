from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy("product_id")
w2 = Window.partitionBy("promo")

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select("id", "coupon_codes", F.col("item.product_id"), F.col("item.promo"))
)

cleaned = (
    exploded
    .filter(F.col("promo").isNotNull())
    .select(
        "*",
        F.array_join("coupon_codes", ", ").alias("coupon_codes_text"),
        F.coalesce("promo", F.lit("UNKNOWN")).alias("promo_clean"),
    )
)

enriched = (
    cleaned
    .withColumn("product_id_percent_rank", F.percent_rank().over(w))
    .withColumn("promo_min_product_id", F.min("product_id").over(w2))
    .withColumn("promo_count_product_id", F.count("product_id").over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("product_id") >= 8, "PREMIUM").otherwise("STANDARD").alias("product_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
        F.percentile_approx("product_id_percent_rank", 0.5).alias("product_id_percent_rank_p50"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
    )
)
