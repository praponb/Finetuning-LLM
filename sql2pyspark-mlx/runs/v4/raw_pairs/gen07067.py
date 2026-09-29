from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(F.quarter("order_date").alias("order_quarter"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") < 54)
    .select("order_quarter", "avg_amount")
    .orderBy(F.col("avg_amount").desc(), "order_quarter")
    .limit(10)
)
