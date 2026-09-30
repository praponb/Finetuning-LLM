from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") < 1100)
    .orderBy("status")
)
