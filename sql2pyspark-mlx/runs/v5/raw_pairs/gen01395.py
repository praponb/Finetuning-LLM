from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(~F.col("t.merchant").isin(["Grab", "Lazada", "Shell"]))
    .groupBy(F.date_trunc("MONTH", F.col("a.opened_date")).alias("opened_month"))
    .agg((F.max(F.col("t.amount")) - F.min(F.col("t.amount"))).alias("amount_range"))
)
