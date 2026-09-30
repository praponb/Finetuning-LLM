from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.month("paid_at").alias("paid_month"),
        F.year("paid_at").alias("paid_year"),
    )
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)
