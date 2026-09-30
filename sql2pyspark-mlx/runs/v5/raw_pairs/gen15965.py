from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") == "DE")
    .select(
        F.col("s.status"),
        F.col("s.end_date"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
    )
    .orderBy("end_date")
)
