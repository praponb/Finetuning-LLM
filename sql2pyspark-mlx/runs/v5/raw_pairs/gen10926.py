from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

remaining_keys = (
    df_shipments
    .select("carrier", "status")
    .intersectAll(
        df_shipments
        .filter(F.col("cost").between(30, 85))
        .select("carrier", "status")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("delivered_date").desc_nulls_last())
)
