from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("order_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isin([20, 27, 29]))
        .select("order_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "order_id", "product_id", "quantity")
    .orderBy(F.col("quantity").asc_nulls_first())
)
