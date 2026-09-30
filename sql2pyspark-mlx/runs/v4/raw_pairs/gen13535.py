from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("status") != "ACTIVE").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(~F.col("c.is_active"))
    .select(F.col("c.id"), F.col("c.segment"))
    .orderBy(F.col("id").desc())
)
