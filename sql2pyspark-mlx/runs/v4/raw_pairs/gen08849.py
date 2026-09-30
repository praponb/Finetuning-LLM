from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left_anti")
    .select(F.col("a.id"), F.col("a.opened_date"), F.col("a.customer_id"))
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(20)
)
