from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.count("amount").alias("amount_count"))
    .orderBy(F.col("paid_month").desc())
)
