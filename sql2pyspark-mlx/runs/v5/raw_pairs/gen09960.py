from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id", "product_id")
    .intersectAll(
        df_order_items
        .filter(F.col("order_id").isin([2, 24, 39]))
        .select("id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "quantity", "unit_price")
    .orderBy("quantity")
)
