from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

matched_keys = (
    df_shipments
    .select("id", "order_id")
    .exceptAll(
        df_shipments
        .filter(F.col("delivered_date") >= "2023-06-01")
        .select("id", "order_id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("status").rlike("^(IN_TRANSIT|DELIVERED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("delivered_date").desc_nulls_last())
)
