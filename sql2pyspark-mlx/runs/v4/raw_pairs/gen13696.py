from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.col("a.status").isNotNull())
    .groupBy(
        F.when(F.col("c.credit_limit") >= 15700, "high").when(F.col("c.credit_limit") >= 5700, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.is_active"),
    )
    .agg(
        F.max(F.col("a.balance")).alias("max_balance"),
        F.min(F.col("a.opened_date")).alias("first_opened_date"),
    )
)
