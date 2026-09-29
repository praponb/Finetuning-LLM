from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
        F.avg(F.col("a.balance")).alias("avg_balance"),
    )
    .orderBy("avg_balance")
)
