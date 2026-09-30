from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("customer_id") == 5).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.city") == "Singapore")
    .select(F.col("c.id"), F.col("c.age"))
    .orderBy("id")
    .limit(50)
)
