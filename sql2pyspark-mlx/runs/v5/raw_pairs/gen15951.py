from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .groupBy("customer_id")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.credit_limit"),
        F.col("c.country"),
        F.col("t.min_opened_date"),
        F.col("t.avg_balance"),
    )
)
