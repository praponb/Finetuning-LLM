from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 2, 8]) & F.col("account_type").isin(["SAVINGS", "LOAN"]))
    .select("status", F.round("balance", 0).alias("balance_rounded"))
)
