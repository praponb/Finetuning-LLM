from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(220, 390))
    .select("unit_price", "order_id", "product_id")
)
