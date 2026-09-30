from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("amount").between(2100, 3200))
    .withColumn("amount_tier", F.when(F.col("amount") >= 4100, "High").when(F.col("amount") >= 800, "Medium").otherwise("Low"))
)

amount_bucket_summary = (
    valid_transactions
    .groupBy(
        F.when(F.col("amount") >= 4300, "high").when(F.col("amount") >= 1800, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    amount_bucket_summary
    .select("amount_bucket", "total_amount")
    .orderBy("total_amount")
)
