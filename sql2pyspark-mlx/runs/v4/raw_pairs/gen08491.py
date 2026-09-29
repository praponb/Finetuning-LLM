from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 31, 37]))
    .select("id", "unit_price", "order_id", "product_id")
)
