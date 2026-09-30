from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date_month").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id", "status").orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status")

filtered = (
    df_subscriptions
    .filter(F.col("end_date").between("2024-02-01", "2025-04-01") & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        F.month("start_date").alias("start_date_month"),
        F.datediff("start_date", "end_date").alias("days_end_date_to_start_date"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        "start_date_month",
        "days_end_date_to_start_date",
    )
)

kept_keys = (
    deduped
    .select("status", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status") == "PAUSED")
        .select("status", "id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-07-18")))
    .select(
        "*",
        F.round((F.col("days_end_date_to_start_date") * 1.1), 2).alias("days_end_date_to_start_date_scaled"),
        (F.col("start_date_month") / F.nullif("days_end_date_to_start_date", F.lit(0))).alias("start_date_month_per_days_end_date_to_start_date"),
    )
)

scored = (
    valid
    .filter(F.col("days_end_date_to_start_date").between(39, 79))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "start_date_month",
        "days_end_date_to_start_date_scaled",
        "start_date_month_per_days_end_date_to_start_date",
        F.avg("days_end_date_to_start_date_scaled").over(w3).alias("status_avg_days_end_date_to_start_date_scaled"),
    )
)

cleaned = (
    scored
    .filter(F.col("end_date") > F.lit("2023-08-07").cast("date"))
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "days_end_date_to_start_date_scaled",
        "status_avg_days_end_date_to_start_date_scaled",
        F.least("status_avg_days_end_date_to_start_date_scaled", "monthly_fee").alias("least_status_avg_days_end_date_to_start_date_scaled_monthly_fee"),
        F.col("status_avg_days_end_date_to_start_date_scaled").cast("string").alias("status_avg_days_end_date_to_start_date_scaled_num"),
    )
)

result = (
    cleaned
    .select(
        "days_end_date_to_start_date_scaled",
        "least_status_avg_days_end_date_to_start_date_scaled_monthly_fee",
        "status_avg_days_end_date_to_start_date_scaled_num",
    )
)
