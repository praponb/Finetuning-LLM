from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 7, 9]))
    .select("revenue")
    .subtract(df_order_items.select(F.col("unit_price").alias("revenue")))
)
