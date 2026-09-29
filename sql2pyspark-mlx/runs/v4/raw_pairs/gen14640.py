from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(F.col("c.name"), F.col("c.age"), F.col("s.customer_id"))
    .orderBy(F.col("customer_id").desc())
)
