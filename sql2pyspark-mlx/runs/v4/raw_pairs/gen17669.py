from pyspark.sql import functions as F

filtered_accounts = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("balance") <= 57000) & (F.col("opened_date") <= "2024-03-01"))
)

summary = (
    filtered_accounts
    .groupBy("customer_id", "status")
    .agg(
        F.round(F.stddev("balance"), 2).alias("stddev_balance"),
        F.round(((100 * F.sum(F.when(F.col("status") == "OPEN", 1).otherwise(0))) / F.count("*")), 1).alias("pct_open"),
    )
)

result = (
    summary
    .select("customer_id", "status", "stddev_balance", "pct_open")
    .orderBy("pct_open")
)
