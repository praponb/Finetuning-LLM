from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method", F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 1)
    .orderBy("paid_month", F.col("cnt").desc())
)
