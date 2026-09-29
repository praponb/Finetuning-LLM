from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (~F.col("x.merchant").isin(["Amazon", "7-Eleven"])), "left_anti")
)
