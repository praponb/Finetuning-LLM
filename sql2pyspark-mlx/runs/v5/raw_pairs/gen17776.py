from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "CANCELLED"])
    .agg(F.avg("amount"))
    .orderBy("channel")
)
