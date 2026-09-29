from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

joined = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id").eqNullSafe(F.col("b.id")), "inner")
    .select(
        "s.*",
        F.col("b.shipping_fee"),
        F.col("b.status").alias("order_status"),
        F.col("b.channel"),
    )
)

result = (
    joined
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        "order_status",
        "channel",
        F.when(F.col("status").rlike("^(RETURNED|DELIVERED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(IN_TRANSIT|LOST)$"), "PRIORITY").otherwise("GROUP_A").alias("status_zone"),
        F.col("min_order_item_quantity"),
    )
)
