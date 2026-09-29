from pyspark.sql import functions as F

discount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("discount").alias("sum_order_discount"))

matched_keys = (
    df_inventory
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") >= 79)
        .select("product_id", "id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(discount_by_store_id, on="store_id", how="left")
    .select("store_id", "last_restock", F.col("sum_order_discount"))
    .orderBy("last_restock")
)
