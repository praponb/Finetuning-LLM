from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("c.city") == "London")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 12800, "high").when(F.col("c.credit_limit") >= 7400, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.segment"),
    )
    .agg(
        F.min(F.col("t.amount")).alias("min_amount"),
        F.count(F.when(F.col("t.merchant") == "Starbucks", 1)).alias("starbucks_count"),
    )
    .orderBy("min_amount")
)
