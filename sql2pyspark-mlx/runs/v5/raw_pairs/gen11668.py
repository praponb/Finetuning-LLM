from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.age") > 65)
    .groupBy(F.col("c.is_active"))
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("row_count").desc())
)
