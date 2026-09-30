from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(150, 220))
    .select("order_id", "unit_price", "product_id")
)
