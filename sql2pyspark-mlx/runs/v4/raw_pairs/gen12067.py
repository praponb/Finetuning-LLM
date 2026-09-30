from pyspark.sql import functions as F

result = (
    df_accounts.alias("accounts")
    .join(df_customers.alias("customers"), F.col("accounts.customer_id") == F.col("customers.id"), "right")
    .filter(F.lower(F.col("customers.name")).like("%enr%"))
    .groupBy(F.col("customers.city"), F.col("customers.segment"))
    .agg(
        F.count(F.when(F.col("accounts.status") == "FROZEN", 1)).alias("frozen_count"),
        F.sum(F.when(F.col("accounts.account_type") == "SAVINGS", F.col("accounts.balance")).otherwise(0)).alias("savings_balance"),
    )
    .orderBy("savings_balance", "city", "segment")
    .limit(5)
)
