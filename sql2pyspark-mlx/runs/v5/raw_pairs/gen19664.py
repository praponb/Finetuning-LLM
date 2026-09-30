from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("product_id").isNull())
        .select("id", "order_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("id", "order_id", "product_id", "unit_price")
    .orderBy(F.col("product_id").asc_nulls_first())
)
