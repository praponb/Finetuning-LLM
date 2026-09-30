from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("balance") > 61000)
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.min("balance").alias("min_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.segment"), F.col("C.avg_balance"), F.col("C.min_balance"))
)
