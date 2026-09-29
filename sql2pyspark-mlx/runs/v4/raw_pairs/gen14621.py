from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull() & F.col("product_id").isin([3, 27, 36, 38]) & (F.col("product_id") == 32))
    .select("id", "unit_price", F.round("quantity", 0).alias("quantity_rounded"))
)
