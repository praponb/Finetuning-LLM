from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

matched_keys = (
    df_payments
    .select("order_id", "method")
    .subtract(
        df_payments
        .filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))
        .select("order_id", "method")
    )
)

verified = (
    df_payments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("method").asc_nulls_last())
)
