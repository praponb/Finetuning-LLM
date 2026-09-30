from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city") != "New York")
    .select(
        F.col("s.status"),
        F.col("s.end_date"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
    .orderBy("status")
)
