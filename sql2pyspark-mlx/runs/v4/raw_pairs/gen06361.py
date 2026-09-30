from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("amount").between(410, 520) & (F.col("paid_at") < "2024-10-16"))
)

summary = (
    recent_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    summary
    .select("status", "card_count", "min_amount")
    .orderBy("card_count")
)
