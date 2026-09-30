from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(260, 290) & F.col("product_id").isin([23, 35]))
    .select("product_id", "order_id")
)
