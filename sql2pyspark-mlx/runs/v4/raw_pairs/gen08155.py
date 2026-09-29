from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("c.signup_date"),
        F.add_months(F.col("s.start_date"), 6).alias("start_date_plus_months"),
    )
)
