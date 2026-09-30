from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("order_id", "product_id")
    .intersect(
        df_order_items
        .filter(F.col("quantity") < 7)
        .select("order_id", "product_id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("id", "order_id", "product_id", "quantity", "unit_price")
    .orderBy(F.col("quantity").asc_nulls_last())
)
