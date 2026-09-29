from pyspark.sql import functions as F

shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "v"),
    )
)

flattened = (
    exploded
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        F.explode("coupon_codes").alias("value"),
    )
)

cleaned = (
    flattened
    .filter(F.col("created_at") >= "2023-07-01")
    .select(
        "id",
        "customer_id",
        "value",
        F.when(F.col("key").isin(["device", "gift"]), "Tier 1").otherwise("Other").alias("key_group"),
        F.concat("key", F.lit(" "), "value").alias("key_value"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.customer_id",
        "a.key_group",
        "a.key_value",
        F.col("p.country"),
        F.col("p.segment"),
        F.col("p.name"),
    )
)

enriched_carts = (
    enriched
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select(
        "a.customer_id",
        "a.key_group",
        "a.key_value",
        F.col("p.segment").alias("customer_segment"),
        F.col("p.email"),
    )
)

summary = (
    enriched_carts
    .groupBy("customer_id")
    .agg(
        F.size(F.collect_set("customer_segment")).alias("num_customer_segments"),
        F.size(F.collect_set("customer_segment")).alias("num_customer_segments_2"),
        F.count(F.when(F.col("customer_segment") == "Consumer", 1)).alias("consumer_count"),
        F.count(F.when(F.col("customer_segment") == "SMB", 1)).alias("smb_count"),
    )
)

result = (
    summary
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "customer_id",
        "num_customer_segments",
        "smb_count",
        F.col("avg_order_shipping_fee"),
    )
    .orderBy("customer_id", "num_customer_segments")
    .limit(5)
)
