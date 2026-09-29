from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sum(F.when(F.col("a.account_type") == "SAVINGS", F.col("a.balance")).otherwise(0)).alias("savings_balance"),
    )
)
