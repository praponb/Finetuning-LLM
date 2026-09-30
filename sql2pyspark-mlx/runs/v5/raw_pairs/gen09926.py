from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.round(F.avg(F.col("accounts.balance")), 2).alias("avg_balance"),
        F.max(F.col("accounts.opened_date")).alias("latest_opened_date"),
    )
    .orderBy(F.col("latest_opened_date").desc(), "is_active")
    .limit(5)
)
