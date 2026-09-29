from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.is_active"),
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("a.status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
        F.sum(F.when(F.col("a.status") == "FROZEN", 1).otherwise(0)).alias("frozen_count"),
    )
)
