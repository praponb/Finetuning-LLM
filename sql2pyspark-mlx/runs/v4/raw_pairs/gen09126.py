from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("customer_id").isNotNull()).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("c.credit_limit"),
        F.col("c.signup_date"),
    )
)
