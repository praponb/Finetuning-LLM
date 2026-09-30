from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

exploded = flattened.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    exploded
    .filter(F.col("code").isNotNull())
    .withColumn("code_upper", F.upper("code"))
)

result = (
    filtered
    .groupBy("code")
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("event_count"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("code") == "BUNDLE", 1)).alias("bundle_rows"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
    )
    .filter(F.col("bundle_rows") > 1)
)
