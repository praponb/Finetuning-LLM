from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["COMPLETED", "CANCELLED", "SHIPPED", "RETURNED"])
    .agg(F.avg("discount"))
)
