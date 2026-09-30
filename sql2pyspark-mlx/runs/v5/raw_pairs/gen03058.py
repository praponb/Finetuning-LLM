from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count("*").alias("num_sessions"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.num_sessions") > 9)
    .select(F.col("c.country"), F.col("c.age"), F.col("cs.num_sessions"))
    .orderBy(F.col("num_sessions").desc())
)
