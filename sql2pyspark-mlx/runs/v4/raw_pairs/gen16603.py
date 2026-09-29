from pyspark.sql import functions as F

matched = (
    df_accounts
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.opened_date") >= F.col("sub.start_date")) & (F.col("a.opened_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .groupBy(
        F.when(F.col("subscription_id") >= 10, "PREMIUM").otherwise("STANDARD").alias("subscription_id_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("num_rows").desc())
)
