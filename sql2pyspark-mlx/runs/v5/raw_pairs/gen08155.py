from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("days_since_paid_at", F.datediff(F.current_date(), "paid_at"))
    .orderBy("amount")
)
