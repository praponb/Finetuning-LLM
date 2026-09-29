from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.opened_date") > "2024-04-01")
    .groupBy(
        F.month(F.col("a.opened_date")).alias("opened_month"),
        F.col("a.account_type"),
    )
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
    .orderBy("account_type")
)
