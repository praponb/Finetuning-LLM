from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([7, 40]))
    .select("shipping_fee")
    .union(df_store_sales.select("revenue"))
    .distinct()
)
