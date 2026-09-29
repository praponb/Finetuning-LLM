from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.min("balance").alias("min_balance"),
        F.avg("balance").alias("avg_balance"),
    )
    .orderBy(F.col("total_balance").desc(), "status")
    .limit(5)
)
