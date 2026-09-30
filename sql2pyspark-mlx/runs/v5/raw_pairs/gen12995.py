from pyspark.sql import functions as F

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-10-18").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.date_add("end_date", 19).alias("end_date_plus_19d"),
    )
)

matched_keys = (
    valid
    .select("id", "status")
    .subtract(
        df_subscriptions
        .filter(F.col("start_date").between("2023-10-01", "2025-10-01"))
        .select("id", "status")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("plan") == "premium")
    .select(
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
        F.last_day("start_date").alias("start_date_month_end"),
    )
)

cleaned = (
    base
    .filter(F.col("monthly_fee") > 240)
    .select(
        "end_date",
        "monthly_fee",
        "status",
        "status_title",
        "start_date_month_end",
        F.last_day("start_date_month_end").alias("start_date_month_end_month_end"),
        F.date_format("start_date", "MMM yyyy").alias("start_date_label"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("monthly_fee") >= 216, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_tier"),
        F.date_trunc("MONTH", "start_date_month_end").alias("activity_month"),
        F.date_trunc("QUARTER", "start_date_month_end").alias("quarter_start"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("active_count"),
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
    )
    .filter(F.col("active_count") < 1000)
)
