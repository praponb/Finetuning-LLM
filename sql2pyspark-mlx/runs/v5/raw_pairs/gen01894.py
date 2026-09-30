from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "inner")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.signup_date") > "2023-09-10")
    .groupBy(F.col("customers.country"), F.col("customers.segment"))
    .agg(
        F.min(F.col("transactions.amount")).alias("min_amount"),
        (F.max(F.col("transactions.amount")) - F.min(F.col("transactions.amount"))).alias("amount_range"),
    )
    .orderBy(F.col("amount_range").desc(), "country", "segment")
    .limit(20)
)
