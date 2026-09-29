from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.to_date("started_at") >= "2023-06-01").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.segment"))
)
