from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("product_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("product_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "order_id", "product_id", "quantity", "unit_price")
    .orderBy("id")
)
