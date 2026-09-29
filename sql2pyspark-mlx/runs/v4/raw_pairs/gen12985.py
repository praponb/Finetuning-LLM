from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([6, 20]) & F.col("quantity").between(4, 7))
    .select("id", "quantity", "unit_price", "product_id")
)
