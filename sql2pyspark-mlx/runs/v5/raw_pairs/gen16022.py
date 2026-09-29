from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("status").eqNullSafe(F.lit("CLOSED"))))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.lower("status").alias("status_lower"),
        F.coalesce("status", F.lit("N/A")).alias("status_clean"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .groupingSets(
        [["city", "status"], ["city"], []],
        F.col("d.city").alias("city"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("s.balance")).alias("max_balance"),
        F.grouping_id().alias("level_id"),
    )
)
