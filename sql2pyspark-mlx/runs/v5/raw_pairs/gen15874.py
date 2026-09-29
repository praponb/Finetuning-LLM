from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("accounts.customer_id"), "inner")
    .groupBy(F.month(F.col("customers.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("transactions.txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
    )
    .orderBy("signup_month")
)
