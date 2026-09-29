from pyspark.sql import functions as F

src = df_payments.filter(F.col("amount") >= 1200)

paid_month_summary = (
    src
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(F.count("*").alias("num_payments"))
)

result = (
    paid_month_summary
    .filter(F.col("num_payments") >= 7)
    .select("paid_month", "num_payments")
    .orderBy("paid_month")
    .limit(10)
)
