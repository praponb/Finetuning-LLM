from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.month("paid_at").alias("paid_month"))
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 6500)
)
