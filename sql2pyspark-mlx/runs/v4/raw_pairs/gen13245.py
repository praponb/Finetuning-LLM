from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("monthly_fee") <= 80).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.age").between(34, 63))
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.city"),
        F.col("c.signup_date"),
    )
    .orderBy("id")
)
