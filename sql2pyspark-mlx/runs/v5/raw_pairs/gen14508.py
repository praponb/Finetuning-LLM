from pyspark.sql import functions as F

result = (
    df_accounts
    .groupBy(
        F.when(F.col("balance") >= 71000, "high").when(F.col("balance") >= 23000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("opened_date").alias("first_opened_date"),
    )
    .filter(F.col("avg_balance") < 73000)
    .select("balance_bucket", "avg_balance", "first_opened_date")
    .orderBy(F.col("balance_bucket").desc(), "avg_balance")
)
