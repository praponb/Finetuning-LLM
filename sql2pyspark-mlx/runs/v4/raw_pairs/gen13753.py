from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(F.when(F.col("amount") >= 3800, "high").when(F.col("amount") >= 700, "mid").otherwise("low").alias("amount_bucket"))
    .pivot("merchant", ["Starbucks", "Lazada"])
    .agg(F.count("amount"))
    .orderBy("amount_bucket")
)
