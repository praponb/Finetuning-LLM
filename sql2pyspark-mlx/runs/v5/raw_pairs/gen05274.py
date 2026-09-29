from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("end_date_plus_months").desc(), "id")
w3 = Window.partitionBy("status")
w4 = Window.partitionBy("country", "customer_id").orderBy(F.col("status_sum_monthly_fee_scaled").desc_nulls_last(), "id")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isin([16, 18]))
    .select(
        "*",
        F.add_months("end_date", 3).alias("end_date_plus_months"),
        F.round("monthly_fee", 1).alias("monthly_fee_rounded"),
    )
)

scored = (
    valid
    .select(
        "id",
        "customer_id",
        "monthly_fee_rounded",
        F.sum("monthly_fee_rounded").over(w).alias("cumulative_monthly_fee_rounded"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("monthly_fee").over(w3).alias("status_sum_monthly_fee"),
    )
)

base = (
    scored
    .filter(F.col("customer_id").isNotNull() & (F.col("cumulative_monthly_fee_rounded") <= 345))
    .select(
        "id",
        "customer_id",
        "cumulative_monthly_fee_rounded",
        "event_seq",
        "status_sum_monthly_fee",
        F.round((F.col("status_sum_monthly_fee") * 1.1), 2).alias("status_sum_monthly_fee_scaled"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.signup_date").between("2023-12-01", "2024-07-01"))
    .select(
        "src.*",
        F.col("d.segment"),
        F.col("d.signup_date"),
        F.col("d.credit_limit"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.*",
        F.col("b.country"),
        F.col("b.credit_limit").alias("customer_credit_limit"),
    )
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        F.when(F.col("customer_credit_limit") >= 13500, "PREMIUM").otherwise("STANDARD").alias("customer_credit_limit_tier"),
    )
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.percentile_approx("status_sum_monthly_fee_scaled", [0.25, 0.5, 0.75]).alias("status_sum_monthly_fee_scaled_quartiles"),
        F.max(F.when(F.col("segment") == "SMB", F.col("status_sum_monthly_fee"))).alias("smb_max_status_sum_monthly_fee"),
    )
    .orderBy("smb_max_status_sum_monthly_fee")
)
