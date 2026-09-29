from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.sum("balance").alias("total_balance"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("avg_balance") >= 60000)
)
