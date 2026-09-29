from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 590)
    .select("status", "min_amount")
)
