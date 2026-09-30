from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").isNotNull() & ~F.col("account_type").isin(["LOAN", "SAVINGS", "CHECKING"]))
    .groupBy("account_type")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
    .filter(F.col("avg_balance") > 77)
    .orderBy("account_type")
)
