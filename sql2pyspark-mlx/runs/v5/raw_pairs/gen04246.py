from pyspark.sql import functions as F

kept_keys = (
    df_order_items
    .select("order_id", "product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("quantity").between(5, 6))
        .select("order_id", "product_id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("id", "quantity")
    .orderBy(F.col("quantity").asc_nulls_last())
)
