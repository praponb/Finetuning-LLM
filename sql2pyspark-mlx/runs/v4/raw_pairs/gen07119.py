from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("k", "value"))
)

valid = (
    flattened
    .filter(F.col("k") == "gift")
    .withColumn("is_welcome5", F.array_contains("coupon_codes", "WELCOME5"))
)

base = (
    valid
    .filter(F.col("k").isNotNull())
    .withColumn("k_lower", F.lower("k"))
    .withColumn("resolved_k", F.coalesce("k", F.lit("OTHER")))
)

result = (
    base
    .select("coupon_codes", "k", "value", "k_lower")
    .orderBy(F.col("k").desc())
)
