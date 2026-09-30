from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "bank_transfer", "paypal"]) & (F.col("order_id").isin([5, 28]) | F.col("method").isNotNull()))
)

agg = (
    recent_payments
    .groupBy("status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    agg
    .filter(F.col("min_amount") < 1380)
    .select("status", "amount_range", "min_amount")
    .orderBy("status")
    .limit(5)
)
