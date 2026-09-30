from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(260, 370))
    .select("id", "product_id", "unit_price")
)
