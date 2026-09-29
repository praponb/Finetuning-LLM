from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "attr_value"),
    )
)

flattened = lines.select("id", "key", F.explode("coupon_codes").alias("value"))

filtered = (
    flattened
    .filter(F.col("value").isNotNull())
    .withColumn("value_key_key", F.concat_ws("-", "value", "key"))
)

filtered_carts = (
    filtered
    .filter(F.col("key").isNotNull())
    .withColumn("key_upper", F.upper("key"))
)

result = (
    filtered_carts
    .groupBy("key")
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_rows"),
        F.count(F.when(F.col("value") == "SPRING10", 1)).alias("spring10_count"),
    )
)
