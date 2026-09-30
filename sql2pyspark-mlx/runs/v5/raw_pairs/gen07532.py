from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("status").isNotNull() & (F.col("status").eqNullSafe(F.lit("FROZEN"))))
    .select(
        "*",
        F.substring("account_type", 1, 2).alias("account_type_prefix"),
        F.coalesce("status", F.lit("DIRECT")).alias("status_filled"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("activity_month"))
    .agg(
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.round(F.sum("balance") / F.nullif(F.count(F.when(F.col("status") == "OPEN", 1)), F.lit(0)), 2).alias("balance_per_open"),
        F.round(F.avg("balance"), 2).alias("avg_balance"),
    )
    .orderBy("distinct_customer_ids")
)
