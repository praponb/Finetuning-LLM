from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .groupBy(F.year(F.col("accounts.opened_date")).alias("opened_year"))
    .agg(
        F.sort_array(F.collect_set(F.col("transactions.merchant"))).alias("merchant_list"),
    )
)
