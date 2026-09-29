from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy(F.col("cumulative_monthly_fee_rounded").desc_nulls_last())
w_hist = Window.partitionBy("customer_id")

cleaned = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-09-15").cast("date"))
    .select(
        "id",
        "customer_id",
        "end_date",
        F.round("monthly_fee", 2).alias("monthly_fee_rounded"),
    )
)

scored = (
    cleaned
    .filter(F.col("end_date") >= "2025-10-01")
    .select(
        "*",
        F.sum("monthly_fee_rounded").over(w).alias("cumulative_monthly_fee_rounded"),
        F.first("monthly_fee_rounded", ignorenulls=True).over(w).alias("first_known_monthly_fee_rounded"),
    )
)

remaining_keys = (
    scored
    .select("id", "customer_id")
    .intersect(
        df_subscriptions
        .filter(F.col("plan") != "enterprise")
        .select("id", "customer_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .select(
        "*",
        F.percent_rank().over(w_ordered).alias("cumulative_monthly_fee_rounded_percent_rank"),
        F.min("cumulative_monthly_fee_rounded").over(w_hist).alias("customer_id_min_cumulative_monthly_fee_rounded"),
        F.rank().over(w_ordered).alias("cumulative_monthly_fee_rounded_rank"),
    )
)

base = (
    with_windows
    .filter(F.col("customer_id_min_cumulative_monthly_fee_rounded").between(170, 340) & (F.col("end_date") >= F.to_date(F.lit("2023-10-21"))))
    .select(
        "customer_id",
        "end_date",
        "cumulative_monthly_fee_rounded",
        "first_known_monthly_fee_rounded",
        "cumulative_monthly_fee_rounded_rank",
        F.least("cumulative_monthly_fee_rounded_rank", "customer_id_min_cumulative_monthly_fee_rounded").alias("least_cumulative_monthly_fee_rounded_rank_customer_id_min_cumulative_monthly_fee_rounded"),
        F.quarter("end_date").alias("end_date_quarter"),
    )
)

stats = (
    base
    .groupBy(
        F.when(F.col("cumulative_monthly_fee_rounded") >= 305, "HIGH").otherwise("LOW").alias("cumulative_monthly_fee_rounded_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.sum("end_date_quarter").alias("total_end_date_quarter"),
    )
)

result = (
    stats
    .select(
        "cumulative_monthly_fee_rounded_tier",
        "distinct_customer_ids",
        "total_end_date_quarter",
    )
    .orderBy(F.col("cumulative_monthly_fee_rounded_tier").desc())
)
