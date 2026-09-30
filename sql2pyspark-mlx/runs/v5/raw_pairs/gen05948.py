from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.cnt") > 4)
    .select(F.col("c.id"), F.col("c.is_active"), F.col("c.age"), F.col("t.cnt"))
    .orderBy(F.col("cnt").desc())
)
