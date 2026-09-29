from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.email").like("%@outlook.com"))
    .select(
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("c.signup_date"),
        F.lower(F.col("c.segment")).alias("segment_lower"),
    )
    .orderBy(F.col("plan").desc())
)
