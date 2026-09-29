from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.month("signup_date").isin([1, 2, 4])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.plan") == "basic")
    .select(F.col("s.id"), F.col("s.monthly_fee"), F.col("s.customer_id"))
)
