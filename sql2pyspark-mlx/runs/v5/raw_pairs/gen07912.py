from pyspark.sql import functions as F

unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.min("unit_price").alias("min_order_item_unit_price"))

joined = (
    df_payments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.customer_id") == 1)
    .select("s.*", F.col("b.order_date"))
)

result = (
    joined
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "status",
        "order_date",
        F.col("min_order_item_unit_price"),
    )
)
