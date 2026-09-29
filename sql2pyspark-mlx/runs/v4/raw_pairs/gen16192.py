from pyspark.sql import functions as F

customer_summary = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.count("balance").alias("balance_count"),
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("cs.balance_count"),
        F.col("cs.stddev_balance"),
    )
    .orderBy("balance_count", "city")
)
