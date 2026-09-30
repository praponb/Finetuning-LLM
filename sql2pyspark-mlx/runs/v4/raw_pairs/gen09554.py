from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 9)
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") >= 490)
    .drop("avg_amount")
    .orderBy(F.col("status").desc(), F.col("min_amount").desc())
)
