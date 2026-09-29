from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

flattened = exploded.select("id", "attr_key", "v", F.explode("coupon_codes").alias("code"))

valid = (
    flattened
    .filter(F.col("attr_key").isin(["utm_source", "gift", "device"]))
    .select("*", F.lower("code").alias("code_lower"))
)

cleaned = (
    valid
    .filter(F.col("code").isNotNull() & ~F.col("attr_key").isin(["weight_kg", "gift", "device"]))
    .select(
        "v",
        "code",
        "code_lower",
        F.concat_ws("-", "attr_key", "code").alias("attr_key_code_key"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("code").isNotNull())
    .withColumn("clean_code", F.trim("code"))
    .withColumn("is_bundle", F.when(F.col("code") == "BUNDLE", 1).otherwise(0))
)

base = (
    cleaned_carts
    .filter(F.col("code").isNotNull())
    .withColumn("is_bundle_adjusted", F.round((F.col("is_bundle") * 1.07), 2))
    .withColumn("code_length", F.length("code"))
    .withColumn("code_clean", F.coalesce("code", F.lit("OTHER")))
)

result = (
    base
    .groupBy("code")
    .agg(
        F.avg(F.when(F.col("code") == "BUNDLE", F.col("code_length"))).alias("bundle_avg_code_length"),
        F.sum(F.when(F.col("code") == "WELCOME5", F.col("is_bundle_adjusted")).otherwise(0)).alias("welcome5_is_bundle_adjusted"),
        F.sum(F.when(F.col("code") == "SPRING10", F.col("is_bundle_adjusted")).otherwise(0)).alias("spring10_is_bundle_adjusted"),
    )
    .filter(F.col("bundle_avg_code_length") < 0)
    .orderBy("spring10_is_bundle_adjusted")
)
