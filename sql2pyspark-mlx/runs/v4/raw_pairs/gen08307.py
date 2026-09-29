from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("end_date") <= "2025-08-23").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.is_active"))
    .select(F.col("c.id"), F.col("c.name"), F.col("c.age"))
    .orderBy("name")
)
