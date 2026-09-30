from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("end_date") > "2025-03-01").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(
        F.col("c.id"),
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)
