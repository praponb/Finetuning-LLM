from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())

cleaned = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .withColumn("start_date_plus_months", F.add_months("start_date", 6))
)

matched_keys = (
    cleaned
    .select("plan")
    .subtract(
        df_subscriptions
        .filter(F.col("status") != "PAUSED")
        .select("plan")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .filter(F.col("start_date").between("2023-04-09", "2024-08-01"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.cume_dist().over(w).alias("monthly_fee_cume_dist"),
    )
)

cleaned_subscriptions = (
    metrics
    .filter(F.col("end_date") > "2025-05-18")
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        F.year("start_date").alias("start_date_year"),
    )
)

filtered = (
    cleaned_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_year",
        F.greatest("start_date_year", "monthly_fee").alias("greatest_start_date_year_monthly_fee"),
        F.datediff("end_date", "start_date").alias("days_start_date_to_end_date"),
    )
)

result = (
    filtered
    .select(
        "customer_id",
        "end_date",
        "greatest_start_date_year_monthly_fee",
        "days_start_date_to_end_date",
    )
    .orderBy(F.col("days_start_date_to_end_date").desc_nulls_first())
)
