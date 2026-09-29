from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.month(F.col("a.opened_date")).isin([1, 8, 11]))
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14000, "high").when(F.col("c.credit_limit") >= 7800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.round(F.stddev(F.col("a.balance")), 2).alias("stddev_balance"))
    .orderBy("credit_limit_bucket", F.col("stddev_balance").desc())
)
