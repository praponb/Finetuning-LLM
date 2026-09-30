from pyspark.sql import functions as F

discount_by_store_id = df_orders.groupBy("store_id").agg(F.max("discount").alias("max_order_discount"))

matched_keys = (
    df_orders
    .select("id", "customer_id")
    .intersectAll(
        df_orders
        .filter(F.col("store_id").isin([3, 4, 11]))
        .select("id", "customer_id")
    )
)

verified = (
    df_orders
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(discount_by_store_id, on="store_id", how="left")
    .select(
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("max_order_discount"),
    )
    .orderBy(F.col("discount").desc())
)
