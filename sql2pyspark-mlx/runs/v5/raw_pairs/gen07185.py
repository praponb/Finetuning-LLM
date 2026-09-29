from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("customer_id").isin([5, 26]))
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.chrome_count") > 8)
    .select(F.col("c.country"), F.col("c.age"), F.col("t.chrome_count"))
    .orderBy("country")
)
