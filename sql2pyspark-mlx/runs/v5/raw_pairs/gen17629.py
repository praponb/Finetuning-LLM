from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("opened_date") >= F.lit("2023-08-19").cast("date")))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["segment", "city"], ["segment"]],
        F.col("p.segment").alias("segment"),
        F.col("p.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.balance")), 2).alias("total_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("segment").asc_nulls_last())
)
