from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([9, 25]) & F.col("order_id").isin([10, 29]))
    .select("id", "quantity", "product_id", "order_id")
)
