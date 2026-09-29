from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter(F.col("customer_id").isNotNull())
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

valid = (
    cleaned
    .filter(F.col("opened_date") >= F.lit("2023-09-30").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.add_months("opened_date_month_end", 6).alias("opened_date_month_end_plus_months"),
        F.coalesce("status", F.lit("OTHER")).alias("resolved_status"),
    )
)

cleaned_accounts = (
    valid
    .filter(F.col("opened_date") >= F.lit("2024-02-28").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.date_add("opened_date", 5).alias("opened_date_plus_5d"),
    )
)

result = (
    cleaned_accounts
    .groupBy(
        F.date_trunc("month", "opened_date_plus_5d").alias("activity_month"),
        "customer_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("balance")).otherwise(0)).alias("credit_balance"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.size(F.collect_set("account_type")).alias("num_account_types_2"),
    )
    .orderBy(F.col("activity_month").desc_nulls_first())
)
