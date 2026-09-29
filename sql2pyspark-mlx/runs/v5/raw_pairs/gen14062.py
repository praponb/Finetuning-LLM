from pyspark.sql import functions as F

exploded = df_carts.select("id", "coupon_codes", F.inline("items"))

flattened = exploded.select("id", "price", F.explode_outer("coupon_codes").alias("code"))

cleaned = (
    flattened
    .filter(F.col("code").isNotNull())
    .withColumn("code_lower", F.lower("code"))
    .withColumn("adjusted_price", F.round((F.col("price") * 1.07), 2))
    .withColumn("code_filled", F.coalesce("code", F.lit("OTHER")))
)

filtered = (
    cleaned
    .filter(F.col("price") >= 150)
    .select(
        "id",
        "code",
        "adjusted_price",
        (F.col("adjusted_price") + F.col("price")).alias("adjusted_price_plus_price"),
        F.least("price", "adjusted_price").alias("least_price_adjusted_price"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("adjusted_price") >= 84, "LARGE").otherwise("SMALL").alias("adjusted_price_tier"),
    )
    .agg(
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_count"),
        F.round(F.sum("adjusted_price") / F.nullif(F.count(F.when(F.col("code") == "WELCOME5", 1)), F.lit(0)), 2).alias("adjusted_price_per_welcome5"),
        F.max(F.when(F.col("code") == "VIP", F.col("least_price_adjusted_price"))).alias("vip_max_least_price_adjusted_price"),
    )
    .orderBy("adjusted_price_tier")
)
