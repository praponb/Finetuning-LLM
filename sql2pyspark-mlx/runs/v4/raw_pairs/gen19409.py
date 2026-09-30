from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1100)
    .groupBy(F.to_date("paid_at").alias("paid_day"), "order_id")
    .agg(
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
    .filter(F.col("promptpay_count") < 65)
    .select("paid_day", "order_id", "promptpay_count")
    .orderBy(F.col("paid_day").desc())
)
