from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(650, 1010))
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"))
    .agg(
        F.sum(F.when(F.col("method") == "paypal", F.col("amount")).otherwise(0)).alias("paypal_amount"),
    )
    .filter(F.col("paypal_amount") >= 30)
    .orderBy("paypal_amount", "paid_month")
    .limit(50)
)
