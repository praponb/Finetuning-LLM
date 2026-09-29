from pyspark.sql import functions as F

quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

kept_keys = (
    df_order_items
    .select("id", "order_id")
    .intersectAll(
        df_order_items
        .filter(F.col("quantity").between(4, 5))
        .select("id", "order_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("order_id").desc_nulls_last())
)
