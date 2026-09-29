from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(~F.col("c.country").isin(["DE", "SG", "US"]))
    .select(F.col("c.id"), F.col("c.name"), F.col("c.segment"))
    .orderBy("name")
)
