from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "RETURNED"])
    .agg(F.sum("amount"))
)
