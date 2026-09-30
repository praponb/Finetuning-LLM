from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .pivot("status", ["SHIPPED", "COMPLETED"])
    .agg(F.max("discount"))
    .orderBy("channel")
)
