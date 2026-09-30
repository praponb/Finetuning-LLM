from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.filter(F.col("account_id") == 19).alias("t"), F.col("a.id") == F.col("t.account_id"), "left_semi")
    .filter(F.col("a.status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(F.col("a.id"), F.col("a.customer_id"))
)
