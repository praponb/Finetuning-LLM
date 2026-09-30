from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.month(F.col("c.signup_date")).isin([6, 7, 11]))
    .groupBy(F.col("c.city"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("a.account_type") == "SAVINGS", 1).otherwise(0)).alias("savings_count"),
    )
)
