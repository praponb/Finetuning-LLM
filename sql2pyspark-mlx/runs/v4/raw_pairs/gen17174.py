from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
    )
    .orderBy(F.col("order_id").desc())
)
