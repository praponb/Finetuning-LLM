from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy("status")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.avg("balance").alias("avg_balance_2"),
    )
    .orderBy(F.col("status").desc())
)
