from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(F.round(F.avg("balance"), 2).alias("avg_balance"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .filter(F.col("cs.avg_balance") > 79)
    .select(F.col("c.id"), F.col("c.credit_limit"), F.col("cs.avg_balance"))
    .orderBy(F.col("id").desc())
)
