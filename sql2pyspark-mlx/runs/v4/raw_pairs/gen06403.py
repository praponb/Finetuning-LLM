from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter((F.col("amount") >= 1500) & (F.col("amount") >= 2300))
    .withColumn("merchant_title", F.initcap(F.trim("merchant")))
)

is_flagged_summary = (
    recent_transactions
    .groupBy("is_flagged")
    .agg(
        F.min("amount").alias("min_amount"),
        F.countDistinct("amount").alias("unique_merchants"),
    )
)

result = (
    is_flagged_summary
    .filter(F.col("min_amount") >= 3900)
    .select("is_flagged", "min_amount")
)
