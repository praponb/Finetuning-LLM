from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") <= 39) & F.col("status").isin(["SHIPPED", "RETURNED", "CANCELLED"]) & (F.col("discount") < 0.32))
    .select("id", "discount", "customer_id", "store_id")
)
