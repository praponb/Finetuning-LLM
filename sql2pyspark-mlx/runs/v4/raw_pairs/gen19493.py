from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.lower(F.col("c.name")).like("%avi%") & (F.col("c.city") == "Singapore"))
    .select(
        F.col("c.city"),
        F.col("c.age"),
        F.col("c.segment"),
        F.col("s.start_date"),
        F.col("s.plan"),
        F.col("s.customer_id"),
    )
)
