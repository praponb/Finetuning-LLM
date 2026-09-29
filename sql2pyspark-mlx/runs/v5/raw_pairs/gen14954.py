from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("max_amount") >= 940)
)
