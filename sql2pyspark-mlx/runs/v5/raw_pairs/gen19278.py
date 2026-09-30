from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") <= 540) & F.col("status").isin(["SHIPPED", "PENDING", "CANCELLED"]) & F.col("store_id").isNull())
)
