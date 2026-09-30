from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.hour("paid_at").between(10, 13))
    .groupBy("method")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 44)
)
