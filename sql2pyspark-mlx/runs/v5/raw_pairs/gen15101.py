from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(~(F.col("c.segment").isin(["Consumer", "Corporate", "SMB"]) & F.col("c.name").like("D%")))
    .select(F.col("c.age"), F.col("s.customer_id"), F.col("s.status"), F.col("s.plan"))
    .orderBy("age")
)
