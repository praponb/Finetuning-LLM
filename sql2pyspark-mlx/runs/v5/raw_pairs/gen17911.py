from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("status") == "CLOSED")
    .groupBy("customer_id")
    .agg(F.max("balance").alias("max_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("ct"), F.col("c.id") == F.col("ct.customer_id"), "inner")
    .select(F.col("c.age"), F.col("ct.max_balance"))
)
