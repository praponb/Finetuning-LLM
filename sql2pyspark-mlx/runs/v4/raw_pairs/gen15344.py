from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.end_date") <= "2023-07-01")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.floor(F.col("s.monthly_fee")).alias("monthly_fee_whole"),
    )
    .orderBy("id")
)
