from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "coupon_codes", F.explode_outer("attributes").alias("key", "v"))
)

flattened_carts = flattened.select("id", "v", F.explode("coupon_codes").alias("value"))

valid = (
    flattened_carts
    .filter(F.col("value").isNull())
    .withColumn("value_group", F.when(F.col("value").isin(["VIP", "SPRING10"]), "Priority").otherwise("Other"))
    .withColumn("resolved_value", F.coalesce("value", F.lit("NONE")))
)

base = (
    valid
    .filter(F.col("value") == "SPRING10")
    .select(
        "v",
        "value_group",
        "resolved_value",
        F.substring("value", 1, 4).alias("value_prefix"),
        F.when(F.col("value") == "BUNDLE", 1).otherwise(0).alias("is_bundle"),
        F.coalesce("value", F.lit("DIRECT")).alias("value_filled"),
    )
)

result = (
    base
    .select("v", "value_group", "resolved_value", "is_bundle", "value_filled")
    .orderBy(F.col("is_bundle").desc())
)
