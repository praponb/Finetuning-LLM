from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("status").isin(["FROZEN", "OPEN"]))
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
        F.max("balance").alias("max_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.age"), F.col("cs.open_balance"), F.col("cs.max_balance"))
)
