from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") <= 13100)
    .select(
        F.col("s.end_date"),
        F.col("s.monthly_fee"),
        F.col("c.city"),
        (F.col("c.age") - F.col("c.credit_limit")).alias("age_minus_credit_limit"),
    )
)
