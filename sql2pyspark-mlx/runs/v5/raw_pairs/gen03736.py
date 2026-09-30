from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .filter(F.col("a.customer_id") == 33)
    .select(F.col("a.id"), F.col("a.balance"))
    .orderBy(F.col("id").desc(), F.col("balance").desc())
    .limit(50)
)
