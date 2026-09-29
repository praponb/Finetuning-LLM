from pyspark.sql import functions as F

filtered = (
    df_accounts
    .filter(F.col("customer_id").isin([4, 12, 14, 21]))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .cube(
        F.col("d.segment").alias("segment"),
        F.col("a.account_type").alias("account_type"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("a.days_since_opened_date")).alias("total_days_since_opened_date"),
    )
    .orderBy(F.col("segment").desc())
)
