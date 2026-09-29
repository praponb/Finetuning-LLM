from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([12, 29, 31]) & F.col("product_id").isin([2, 8, 36, 40]))
    .select("id", "order_id", "product_id", "unit_price")
)
