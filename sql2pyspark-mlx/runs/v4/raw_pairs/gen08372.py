from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("amount") > 460)

paid_month_summary = (
    valid_payments
    .groupBy(F.date_trunc("MONTH", "paid_at").alias("paid_month"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    paid_month_summary
    .filter(F.col("amount_count") > 5)
    .select("paid_month", "total_amount")
    .orderBy("paid_month", F.col("total_amount").desc())
    .limit(20)
)
