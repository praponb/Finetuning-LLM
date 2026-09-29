from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

exploded = flattened.select("id", "key", "value", F.explode("coupon_codes").alias("value_2"))

valid = (
    exploded
    .filter(F.col("value_2").isNotNull() & F.col("key").isin(["weight_kg", "utm_source", "device"]))
    .select("*", F.lower("value_2").alias("value_2_lower"))
)

filtered = (
    valid
    .filter(F.col("value_2").isin(["SPRING10", "WELCOME5", "VIP"]))
    .withColumn("key_value_2", F.concat("key", F.lit(" - "), "value_2"))
    .withColumn("key_clean", F.coalesce("key", F.lit("OTHER")))
)

valid_carts = (
    filtered
    .filter(F.col("key").isin(["utm_source", "weight_kg", "gift"]))
    .select(
        "id",
        "key",
        "value",
        "value_2",
        "value_2_lower",
        F.lower("key").alias("key_lower"),
        F.coalesce("key", F.lit("DIRECT")).alias("resolved_key"),
    )
)

grouped = (
    valid_carts
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_rows"),
        F.count_if(F.col("key") == "weight_kg").alias("weight_kg_events"),
        F.count(F.when(F.col("value_2") == "VIP", 1)).alias("vip_count"),
    )
)

result = (
    grouped
    .select("key", "utm_source_rows", "vip_count")
    .orderBy(F.col("vip_count").asc_nulls_last())
)
