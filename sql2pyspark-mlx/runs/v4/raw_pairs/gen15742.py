from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id") == 5) | (F.col("paid_at") < "2024-08-01"))
    .groupBy("status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("amount").alias("amount_count"),
    )
    .orderBy(F.col("max_amount").desc())
)
