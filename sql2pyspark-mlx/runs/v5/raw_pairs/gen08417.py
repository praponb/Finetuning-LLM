from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.avg("amount").alias("avg_payment_amount"))

enriched = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.discount"), F.col("b.order_date"), F.col("b.shipping_fee"))
)

result = (
    enriched
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "discount",
        F.col("avg_payment_amount"),
    )
    .orderBy(F.col("unit_price").desc())
)
