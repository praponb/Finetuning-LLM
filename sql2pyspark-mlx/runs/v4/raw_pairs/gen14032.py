from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.when(F.col("amount") >= 1370, "high").when(F.col("amount") >= 580, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("method", ["promptpay", "cash", "bank_transfer", "card"])
    .agg(F.count("amount"))
)
