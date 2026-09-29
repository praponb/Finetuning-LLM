from pyspark.sql import functions as F

customer_stats = (
    df_accounts
    .filter(F.col("opened_date") < "2023-10-01")
    .groupBy("customer_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "FROZEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_frozen"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("C"), F.col("c.id") == F.col("C.customer_id"), "inner")
    .filter(F.col("C.pct_frozen") >= 54)
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("C.pct_frozen"),
    )
)
