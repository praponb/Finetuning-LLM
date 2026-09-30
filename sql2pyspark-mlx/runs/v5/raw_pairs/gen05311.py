from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("monthly_fee_share_minus_last_known_monthly_fee", "id")
w_grp = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")
w_ordered_2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w7 = Window.partitionBy("monthly_fee_percent_rank_tier").orderBy(F.col("distinct_ids").desc())

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.upper("plan").alias("plan_upper"),
        F.last_day("end_date").alias("end_date_month_end"),
    )
)

scored = (
    base
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "plan_upper",
        F.sum("monthly_fee").over(w).alias("status_sum_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee"),
    )
)

cleaned = (
    scored
    .filter(F.col("customer_id").isNotNull() & (F.col("status") != "PAUSED"))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "plan_upper",
        "status_sum_monthly_fee",
        "monthly_fee_share",
        F.date_sub("start_date", 8).alias("start_date_minus_8d"),
        (F.col("monthly_fee_share") - F.col("last_known_monthly_fee")).alias("monthly_fee_share_minus_last_known_monthly_fee"),
    )
)

scored_subscriptions = (
    cleaned
    .withColumn("monthly_fee_share_minus_last_known_monthly_fee_bucket", F.ntile(5).over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_grp))
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w_ordered_2))
)

stats = (
    scored_subscriptions
    .groupBy(
        F.when(F.col("monthly_fee_percent_rank") >= 0.65, "HIGH").otherwise("LOW").alias("monthly_fee_percent_rank_tier"),
        "seq_no",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.max("monthly_fee_share_minus_last_known_monthly_fee").alias("max_monthly_fee_share_minus_last_known_monthly_fee"),
        F.percentile_approx("monthly_fee_share_minus_last_known_monthly_fee_bucket", [0.25, 0.5, 0.75]).alias("monthly_fee_share_minus_last_known_monthly_fee_bucket_quartiles"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w7))
)

result = (
    stats
    .select(
        "monthly_fee_percent_rank_tier",
        "seq_no",
        "distinct_ids",
        "distinct_customer_ids",
        "monthly_fee_share_minus_last_known_monthly_fee_bucket_quartiles",
        "distinct_ids_percent_rank",
    )
)
