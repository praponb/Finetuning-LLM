from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") == 31)
    .select(
        F.col("s.id"),
        F.col("s.plan"),
        F.col("s.status"),
        F.col("c.age"),
        F.col("c.segment"),
        F.col("c.is_active"),
    )
    .orderBy(F.col("age").desc())
)
