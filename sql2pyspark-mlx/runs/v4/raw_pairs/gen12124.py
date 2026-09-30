from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_first())
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee_cume_dist_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_cume_dist_2").desc(), "id")
w_grp = Window.partitionBy("plan", "customer_id").orderBy("monthly_fee_cume_dist_2_bucket", "id")
w8 = Window.partitionBy("customer_id", "status").orderBy(F.col("first_known_running_monthly_fee").asc_nulls_first())
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("amount").alias("sum_order_amount"))

scored = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.avg("monthly_fee").over(w2).alias("monthly_fee_avg_5_prior"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
        F.cume_dist().over(w4).alias("monthly_fee_cume_dist_2"),
    )
)

with_windows = (
    scored
    .withColumn("first_known_running_monthly_fee", F.first("running_monthly_fee", ignorenulls=True).over(w5))
    .withColumn("monthly_fee_cume_dist_2_bucket", F.ntile(10).over(w6))
)

with_windows_subscriptions = (
    with_windows
    .select(
        "*",
        F.row_number().over(w_grp).alias("event_seq"),
        F.percent_rank().over(w8).alias("first_known_running_monthly_fee_percent_rank"),
    )
)

base = (
    with_windows_subscriptions
    .filter((F.col("end_date") > F.lit("2024-01-29").cast("date")) & F.col("customer_id").isNotNull() & (F.col("monthly_fee_cume_dist_2_bucket") > 7))
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "monthly_fee_avg_5_prior",
        "monthly_fee_cume_dist",
        "monthly_fee_cume_dist_2",
        "first_known_running_monthly_fee",
        "first_known_running_monthly_fee_percent_rank",
        F.coalesce("monthly_fee_cume_dist_2_bucket", F.lit(0)).alias("monthly_fee_cume_dist_2_bucket_filled"),
    )
)

result = (
    base
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee_avg_5_prior",
        "monthly_fee_cume_dist",
        "first_known_running_monthly_fee",
        "first_known_running_monthly_fee_percent_rank",
        "monthly_fee_cume_dist_2_bucket_filled",
        F.col("sum_order_amount"),
    )
    .orderBy(
        F.col("monthly_fee_cume_dist").desc_nulls_first(),
        "monthly_fee_cume_dist_2_bucket_filled",
    )
)
