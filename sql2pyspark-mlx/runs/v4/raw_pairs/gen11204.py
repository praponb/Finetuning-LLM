from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("a.status") == "CLOSED")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.segment"),
    )
    .agg(
        F.sum(F.when(F.col("a.account_type") == "LOAN", 1).otherwise(0)).alias("loan_count"),
        F.max(F.col("a.opened_date")).alias("latest_opened_date"),
    )
)
