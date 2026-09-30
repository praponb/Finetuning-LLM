from pyspark.sql import functions as F

result = (
    df_orders
    .select("shipping_fee")
    .intersect(df_store_sales.select(F.col("revenue").alias("shipping_fee")))
)
