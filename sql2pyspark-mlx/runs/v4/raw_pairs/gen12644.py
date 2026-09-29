from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("status") == "OPEN", F.col("balance")).otherwise(0)).alias("open_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .select(F.col("c.id"), F.col("c.city"), F.col("C.open_balance"))
    .orderBy("open_balance", F.col("city").desc())
)
