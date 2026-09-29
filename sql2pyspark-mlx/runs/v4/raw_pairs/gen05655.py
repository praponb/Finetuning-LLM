from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("monthly_fee").between(80, 230)).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.segment"), F.col("c.email"))
)
