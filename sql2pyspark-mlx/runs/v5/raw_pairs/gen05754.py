from pyspark.sql import functions as F

quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

remaining_keys = (
    df_inventory
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") <= 231)
        .select("product_id", "store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("id", "product_id", "last_restock", F.col("avg_order_item_quantity"))
    .orderBy(F.col("product_id").asc_nulls_first())
)
