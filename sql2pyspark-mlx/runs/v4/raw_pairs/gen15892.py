from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.name"), F.col("c.segment"), F.col("ct.th_count"))
)
