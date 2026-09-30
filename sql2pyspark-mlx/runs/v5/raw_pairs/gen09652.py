from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.min("balance").alias("min_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.age"), F.col("C.min_balance"))
    .orderBy("age")
)
