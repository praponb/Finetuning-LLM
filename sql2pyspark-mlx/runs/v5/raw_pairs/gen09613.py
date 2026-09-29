from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant").isNotNull()).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.year(F.col("a.opened_date")) == 2025)
    .select(F.col("a.id"), F.col("a.balance"), F.col("a.status"))
)
