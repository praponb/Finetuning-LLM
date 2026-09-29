from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

lines = (
    df_carts
    .select("id", "customer_id", F.explode_outer("attributes").alias("k", "attr_value"))
)

joined = (
    lines
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"))
)

cleaned = (
    joined
    .filter(F.col("country").isNotNull())
    .withColumn("k_lower", F.lower("k"))
)

valid = (
    cleaned
    .filter(F.col("customer_id") == 22)
    .withColumn("k_lower_2", F.lower("k"))
    .withColumn("clean_k", F.trim("k"))
    .withColumn("k_clean", F.coalesce("k", F.lit("DIRECT")))
)

filtered = (
    valid
    .filter(F.col("customer_id").isNull())
    .withColumn("country_k", F.concat("country", F.lit(" - "), "k"))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.age"), F.col("p.credit_limit"), F.col("p.name"))
)

matched_keys = (
    with_dims
    .select("customer_id")
    .subtract(
        df_carts
        .filter(F.array_contains("coupon_codes", "SPRING10"))
        .select("customer_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

pivoted = (
    reconciled
    .groupBy("customer_id")
    .pivot("k", ["device", "gift"])
    .agg(F.max("credit_limit"))
)

result = (
    pivoted
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select("customer_id", "device", "gift", F.col("avg_order_shipping_fee"))
    .orderBy("customer_id")
)
