from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.end_date") > "2024-11-01")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.city"),
    )
    .agg(
        F.count(F.when(F.col("s.plan") == "premium", 1)).alias("premium_count"),
        F.sum(F.when(F.col("s.plan") == "enterprise", 1).otherwise(0)).alias("enterprise_count"),
    )
    .orderBy("signup_month", "city")
    .limit(50)
)
