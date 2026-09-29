from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.count(F.when(F.col("s.status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.count("*").alias("num_subscriptions"),
    )
    .orderBy(F.col("cancelled_count").desc())
)
