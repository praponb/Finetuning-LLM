from pyspark.sql import functions as F

src = df_accounts.filter(F.year("opened_date") == 2025)

accounts_stats = (
    src
    .groupBy("account_type")
    .agg((F.max("balance") - F.min("balance")).alias("balance_range"))
)

result = (
    accounts_stats
    .filter(F.col("balance_range") < 77)
    .select("account_type", "balance_range")
)
