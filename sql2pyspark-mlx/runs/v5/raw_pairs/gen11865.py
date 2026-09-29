from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_accounts.alias("a"), F.col("c.id") == F.col("a.customer_id"), "left")
    .groupBy(F.col("c.country"))
    .agg(
        F.count(F.col("a.id")).alias("account_count"),
        F.max(F.col("a.balance")).alias("max_balance"),
    )
    .orderBy("max_balance", "country")
    .limit(20)
)
