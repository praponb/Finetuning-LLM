from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1210, "high").when(F.col("amount") >= 390, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") >= 76)
)
