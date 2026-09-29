from pyspark.sql import functions as F

exploded = (
    df_carts
    .withColumn("elem", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("elem.price"),
        F.col("elem.promo"),
    )
)

flattened = exploded.select("id", "promo", F.explode_outer("coupon_codes").alias("value"))

filtered = (
    flattened
    .filter(F.col("value").isNotNull() & (F.col("value") == "BUNDLE"))
    .select(
        "*",
        F.concat("value", F.lit(", "), "promo").alias("value_promo"),
        F.coalesce("value", F.lit("N/A")).alias("value_clean"),
    )
)

valid = (
    filtered
    .filter(F.col("value").isNotNull())
    .select(
        "promo",
        "value_promo",
        F.when(F.col("value").isin(["BUNDLE", "VIP"]), "Priority").otherwise("Other").alias("value_group"),
        F.coalesce("value", F.lit("UNKNOWN")).alias("resolved_value"),
    )
)

result = (
    valid
    .groupBy("promo")
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_count"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("promo") == "BUNDLE", 1)).alias("bundle_count"),
        F.count(F.when(F.col("promo") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .orderBy(F.col("bundle_count").desc())
)
