from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.when(F.col("amount") >= 1760, "high").when(F.col("amount") >= 220, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("method", ["paypal", "card"])
    .agg(F.avg("amount"))
)
