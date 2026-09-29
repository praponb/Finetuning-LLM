from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") < 36)
    .groupBy("channel")
    .pivot("status", ["RETURNED", "COMPLETED", "CANCELLED", "SHIPPED"])
    .agg(F.sum("amount"))
)
