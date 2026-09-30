from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") <= 1700)
    .groupBy(F.when(F.col("amount") >= 4200, "high").when(F.col("amount") >= 2000, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("merchant", ["Shell", "Amazon", "7-Eleven", "Starbucks"])
    .agg(F.max("amount"))
    .orderBy("amount_bucket")
)
