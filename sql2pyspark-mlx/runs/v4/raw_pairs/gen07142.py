from pyspark.sql import functions as F

remaining_keys = (
    df_order_items
    .select("id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") == 3)
        .select("id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "quantity", "unit_price")
    .orderBy(F.col("quantity").desc_nulls_first())
)
