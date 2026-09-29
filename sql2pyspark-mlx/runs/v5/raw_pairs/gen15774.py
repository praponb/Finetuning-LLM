from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.count("*").alias("payment_count"))
    .filter(F.col("payment_count") > 8)
)
