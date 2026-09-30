from pyspark.sql import functions as F

src = df_accounts.filter(F.col("opened_date").between("2024-12-01", "2025-07-17"))

accounts_stats = (
    src
    .groupBy("account_type")
    .agg(
        F.max("balance").alias("max_balance"),
        (F.max("balance") - F.min("balance")).alias("balance_range"),
    )
)

result = accounts_stats.orderBy("balance_range")
