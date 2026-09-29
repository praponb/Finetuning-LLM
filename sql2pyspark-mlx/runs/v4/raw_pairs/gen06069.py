from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

remaining_keys = (
    df_shipments
    .select("order_id")
    .intersect(
        df_shipments
        .filter(F.col("cost") <= 84)
        .select("order_id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("carrier").rlike("^(Kerry|DHL)$"), "CORE").otherwise("EXTENDED").alias("carrier_zone"),
        F.col("sum_order_item_quantity"),
    )
    .orderBy("order_id")
)
