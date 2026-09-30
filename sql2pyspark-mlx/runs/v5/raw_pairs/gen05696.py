from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.avg("balance").alias("avg_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("cs.avg_balance"))
    .orderBy(F.col("avg_balance").desc(), F.col("id").desc())
)
