from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.orderBy("end_date", "id")
w3 = Window.partitionBy("status", "plan").orderBy("end_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w5 = Window.partitionBy("customer_id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-02-16"))) & F.col("customer_id").isNotNull())
    .withColumn("end_date_plus_months", F.add_months("end_date", 6))
)

enriched = (
    valid
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2, 0).over(w2))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w3))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w3))
    .withColumn("monthly_fee_bucket", F.ntile(10).over(w4))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.city"))
)

with_windows = (
    with_dims
    .filter(F.month("end_date").isin([5, 8, 9]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "end_date_plus_months",
        "prev_monthly_fee",
        "first_known_monthly_fee",
        "monthly_fee_bucket",
        F.round((F.col("monthly_fee_bucket") / F.sum("monthly_fee_bucket").over(w)), 4).alias("monthly_fee_bucket_share"),
        F.min("monthly_fee_share").over(w5).alias("customer_id_min_monthly_fee_share"),
    )
)

grouped = (
    with_windows
    .groupBy(
        F.date_trunc("YEAR", "end_date_plus_months").alias("year_start"),
        F.when(F.col("prev_monthly_fee") >= 200, "LARGE").otherwise("SMALL").alias("prev_monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("monthly_fee_bucket"))).alias("cancelled_total_monthly_fee_bucket"),
        F.round(F.avg("first_known_monthly_fee"), 2).alias("avg_first_known_monthly_fee"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.count_if(F.col("status") == "ACTIVE").alias("active_events"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
    )
)

result = (
    grouped
    .select(
        "year_start",
        "prev_monthly_fee_tier",
        "cancelled_count",
        "avg_first_known_monthly_fee",
        "active_events",
        "basic_count",
    )
)
