from pyspark.sql import functions as F

valid_transactions = df_transactions.filter(F.col("is_flagged"))

merchant_summary = (
    valid_transactions
    .groupBy("merchant")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
)

result = merchant_summary.select("merchant", "total_amount")
