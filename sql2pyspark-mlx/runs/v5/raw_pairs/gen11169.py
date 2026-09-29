from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 56, "high").when(F.col("c.age") >= 32, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sum(F.col("a.balance")).alias("total_balance"))
    .orderBy(F.col("total_balance").desc())
)
