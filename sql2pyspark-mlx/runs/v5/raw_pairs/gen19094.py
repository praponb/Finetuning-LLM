from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.avg("balance").alias("avg_balance"),
        F.max("balance").alias("max_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.avg_balance") > 68000)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("cs.avg_balance"),
        F.col("cs.max_balance"),
    )
)
