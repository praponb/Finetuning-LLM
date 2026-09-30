from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 58, "high").when(F.col("c.age") >= 27, "mid").otherwise("low").alias("age_bucket"),
        F.year(F.col("c.signup_date")).alias("signup_year"),
    )
    .agg(
        F.sum(F.when(F.col("a.status") == "FROZEN", F.col("a.balance")).otherwise(0)).alias("frozen_balance"),
        F.sum(F.when(F.col("a.account_type") == "SAVINGS", F.col("a.balance")).otherwise(0)).alias("savings_balance"),
    )
    .orderBy("savings_balance", F.col("signup_year").desc())
)
