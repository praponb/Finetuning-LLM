from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.start_date"),
        F.col("s.end_date"),
        F.col("c.country"),
        F.col("c.credit_limit"),
        F.col("c.segment"),
    )
    .orderBy("credit_limit")
)
