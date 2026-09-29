from pyspark.sql import functions as F

customer_totals = (
    df_accounts
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("t.frozen_count"),
    )
)
