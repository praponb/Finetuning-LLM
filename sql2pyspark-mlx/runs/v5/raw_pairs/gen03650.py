from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount").isNotNull())
    .groupBy("status")
    .agg(F.max("amount").alias("max_amount"))
)
