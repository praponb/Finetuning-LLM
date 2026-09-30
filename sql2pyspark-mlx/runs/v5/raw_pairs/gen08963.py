from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1040)
    .groupBy("channel")
    .agg(F.max("order_date").alias("last_order_date"))
    .orderBy(F.col("channel").desc())
)
