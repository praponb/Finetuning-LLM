from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("amount") <= 1380)
    .withColumn("status_lower", F.lower("status"))
)

payments_stats = (
    src
    .groupBy("method")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
    )
)

result = payments_stats.orderBy("success_amount")
