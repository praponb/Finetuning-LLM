from pyspark.sql import functions as F

matched_keys = (
    df_order_items
    .select("product_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([11, 13, 33, 38]))
        .select("product_id", "id")
    )
)

verified = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
    )
)
