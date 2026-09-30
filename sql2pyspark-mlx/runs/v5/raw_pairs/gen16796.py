from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(200, 280))
    .select("id", "unit_price", "product_id", "order_id")
)
