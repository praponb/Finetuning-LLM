from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") > 1270)
    .orderBy("status")
)
