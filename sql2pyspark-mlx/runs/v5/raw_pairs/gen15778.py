from pyspark.sql import functions as F

src = (
    df_transactions
    .filter(F.col("account_id").isin([7, 27]))
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

amount_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("amount") >= 4100, "high").when(F.col("amount") >= 1400, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("merchant") == "Lazada", 1).otherwise(0)).alias("lazada_count"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = amount_bucket_summary
