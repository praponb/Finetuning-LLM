from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "paypal", 1).otherwise(0)).alias("paypal_count"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .filter(F.col("paypal_count") < 66)
)
