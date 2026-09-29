from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .groupBy(
        F.when(F.col("qty") >= 5, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
        "promo",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("price").alias("total_price"),
        F.count_if(F.col("promo") == "WELCOME5").alias("welcome5_events"),
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
