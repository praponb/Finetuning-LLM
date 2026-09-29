from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("merchant").isin(["Lazada", "7-Eleven"])).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.opened_date").between("2023-07-29", "2025-03-01"))
    .select(F.col("a.id"), F.col("a.status"))
)
