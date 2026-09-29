from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .filter(F.year(F.col("c.signup_date")) == 2025)
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
        F.round(((100 * F.sum(F.when(F.col("a.account_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
    )
    .orderBy("pct_credit")
)
