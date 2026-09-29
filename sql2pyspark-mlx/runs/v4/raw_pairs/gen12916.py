from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-07-07").cast("date")) & F.col("customer_id").isNotNull())
    .select("*", F.col("balance").cast("int").alias("balance_int"))
)

result = (
    base
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .groupingSets(
        [["customer_id", "activity_month", "account_type"], ["customer_id", "activity_month"], ["customer_id"]],
        "customer_id",
        "activity_month",
        "account_type",
    )
    .agg(
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.percentile_approx("balance_int", [0.25, 0.5, 0.75]).alias("balance_int_quartiles"),
        F.grouping("customer_id").alias("grouping_id"),
    )
    .orderBy(F.col("credit_rows").desc_nulls_last())
)
