from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_transactions.alias("transactions"), F.col("accounts.id") == F.col("transactions.account_id"), "left_semi")
    .filter(F.year(F.col("accounts.opened_date")) >= 2025)
    .select(F.col("accounts.id"), F.col("accounts.opened_date"))
    .orderBy(F.col("opened_date").desc(), "id")
    .limit(3)
)
