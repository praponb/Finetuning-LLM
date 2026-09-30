from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.uk_count") > 26)
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("t.uk_count"))
    .orderBy("uk_count")
)
