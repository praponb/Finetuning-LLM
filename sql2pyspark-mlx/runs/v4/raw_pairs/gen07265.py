from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

remaining_keys = (
    df_order_items
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") >= 4)
        .select("order_id", "id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        F.col("sum_inventory_quantity"),
    )
    .orderBy(F.col("product_id").desc())
)
