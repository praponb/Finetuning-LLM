from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city").isin(["London", "Chiang Mai"]))
    .select(
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.col("s.plan"),
        F.col("s.end_date"),
    )
    .orderBy("segment")
)
