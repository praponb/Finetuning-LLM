from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

flattened = exploded.select("id", "key", "value", F.explode("coupon_codes").alias("value_2"))

filtered = (
    flattened
    .filter(F.col("key") != "device")
    .select(
        "*",
        F.concat("key", F.lit(" "), "value_2").alias("key_value_2"),
        F.coalesce("value_2", F.lit("DIRECT")).alias("value_2_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("value_2") == "WELCOME5")
    .select(
        "*",
        F.upper("value_2").alias("value_2_upper"),
        F.coalesce("key", F.lit("N/A")).alias("key_filled"),
    )
)

valid_carts = (
    valid
    .filter(F.col("value_2") != "SPRING10")
    .withColumn("value_2_prefix", F.substring("value_2", 1, 4))
    .withColumn("key_lower", F.lower("key"))
    .withColumn("value_2_filled_2", F.coalesce("value_2", F.lit("DIRECT")))
)

filtered_carts = (
    valid_carts
    .filter(F.col("key").isin(["utm_source", "gift", "weight_kg"]))
    .select(
        "id",
        "key",
        "value",
        "value_2",
        "key_value_2",
        "value_2_filled_2",
        F.concat_ws("-", "key", "value_2").alias("key_value_2_key"),
    )
)

base = (
    filtered_carts
    .filter(F.col("key").isNotNull())
    .withColumn("key_group", F.when(F.col("key").isin(["utm_source", "gift"]), "Group A").otherwise("Other"))
    .withColumn("key_filled", F.coalesce("key", F.lit("NONE")))
)

result = (
    base
    .groupBy("value_2")
    .agg(
        F.size(F.collect_set("value_2")).alias("num_value_2s"),
        F.count(F.when(F.col("value_2") == "BUNDLE", 1)).alias("bundle_count"),
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events"),
    )
    .orderBy(F.col("bundle_count").desc_nulls_first())
)
