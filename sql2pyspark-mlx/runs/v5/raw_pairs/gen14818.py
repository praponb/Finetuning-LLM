from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.balance") > 55000)
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.avg(F.col("a.balance")).alias("avg_balance"),
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
    )
    .filter(F.col("avg_balance") >= 24000)
    .orderBy("last_opened_date", "signup_year")
)
