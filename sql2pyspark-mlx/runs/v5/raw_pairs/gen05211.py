from pyspark.sql import functions as F

quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

matched_keys = (
    df_order_items
    .select("id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") > 7)
        .select("id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("id", "order_id", "product_id", F.col("avg_inventory_quantity"))
)
