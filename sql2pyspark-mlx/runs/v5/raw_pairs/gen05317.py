from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
        F.sum(F.col("a.balance")).alias("total_balance"),
    )
    .filter(F.col("total_balance") >= 436000)
)
