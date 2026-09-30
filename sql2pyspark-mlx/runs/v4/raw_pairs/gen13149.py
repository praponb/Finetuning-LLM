from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "coupon_codes", F.explode("attributes").alias("k", "attr_value"))
)

flattened_carts = (
    flattened
    .select("id", "k", "attr_value", F.explode_outer("coupon_codes").alias("tag"))
)

cleaned = (
    flattened_carts
    .filter(~F.col("tag").isin(["WELCOME5", "VIP"]))
    .select(
        "*",
        F.concat_ws("-", "tag", "k").alias("tag_k_key"),
        F.length("tag").alias("tag_length"),
        F.coalesce("k", F.lit("DIRECT")).alias("k_clean"),
    )
)

filtered = (
    cleaned
    .filter(F.col("tag_length").between(61, 78))
    .withColumn("is_device", F.when(F.col("k") == "device", 1).otherwise(0))
    .withColumn("clean_tag", F.trim("tag"))
    .withColumn("k_filled", F.coalesce("k", F.lit("OTHER")))
)

result = (
    filtered
    .select("id", "attr_value", "tag_k_key", "tag_length", "is_device", "k_filled")
    .orderBy(F.col("tag_length").desc_nulls_first())
)
