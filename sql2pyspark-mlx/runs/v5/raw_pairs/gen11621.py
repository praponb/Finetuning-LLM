from pyspark.sql import functions as F

valid_payments = df_payments.filter(F.col("paid_at") < "2024-04-01")

summary = (
    valid_payments
    .groupBy("method")
    .agg(
        F.count("*").alias("num_payments"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = summary.filter(F.col("num_payments") > 9)
