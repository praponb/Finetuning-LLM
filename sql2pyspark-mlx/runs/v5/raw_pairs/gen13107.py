from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2024-01-01")
    .groupBy(F.year("paid_at").alias("paid_year"))
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 65)
)
