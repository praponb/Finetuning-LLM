from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy("start_date_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id")

valid = (
    df_subscriptions
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED", "PAUSED"]) & (F.col("end_date") >= F.to_date(F.lit("2023-05-13"))))
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.year("start_date").alias("start_date_year"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    ranked
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "start_date_year",
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.ntile(10).over(w_running).alias("monthly_fee_bucket"),
    )
)

cleaned = (
    metrics
    .filter(F.col("end_date").between("2024-09-07", "2025-08-01"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "start_date_year",
        "first_known_monthly_fee",
        "monthly_fee_bucket",
        F.round("monthly_fee", 1).alias("monthly_fee_rounded"),
    )
)

cleaned_subscriptions = (
    cleaned
    .filter((F.col("start_date") >= F.lit("2023-07-22").cast("date")) & F.col("start_date_year").between(40, 58))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "start_date_year",
        "monthly_fee_bucket",
        "monthly_fee_rounded",
        (F.col("monthly_fee") - F.col("first_known_monthly_fee")).alias("monthly_fee_minus_first_known_monthly_fee"),
    )
)

valid_subscriptions = (
    cleaned_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-03-15"))) & F.col("start_date").between("2023-03-12", "2025-06-01"))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "start_date_year",
        "monthly_fee_bucket",
        "monthly_fee_rounded",
        "monthly_fee_minus_first_known_monthly_fee",
        (F.col("monthly_fee") / F.nullif("monthly_fee_minus_first_known_monthly_fee", F.lit(0))).alias("monthly_fee_per_monthly_fee_minus_first_known_monthly_fee"),
    )
)

result = (
    valid_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee_rounded") >= 87, "PREMIUM").otherwise("STANDARD").alias("monthly_fee_rounded_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
    )
    .orderBy(F.col("monthly_fee_p90").asc_nulls_first())
)
