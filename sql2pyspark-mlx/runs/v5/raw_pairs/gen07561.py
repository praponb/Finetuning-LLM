from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("email").like("%@yahoo.com")).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.status") == "PAUSED")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.end_date"),
        F.col("s.start_date"),
    )
    .orderBy(F.col("customer_id").desc())
)
