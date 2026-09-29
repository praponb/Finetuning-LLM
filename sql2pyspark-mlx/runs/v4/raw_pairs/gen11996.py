from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 9, 10]))
    .groupBy("account_type")
    .agg(
        F.sum("balance").alias("total_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
    .filter(F.col("total_balance") > 453000)
    .select("account_type", "total_balance", "balance_range")
    .orderBy(F.col("balance_range").desc())
)
