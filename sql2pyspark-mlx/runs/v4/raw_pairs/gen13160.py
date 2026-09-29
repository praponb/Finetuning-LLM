from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2024-03-07").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.ceil("monthly_fee").alias("monthly_fee_whole"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

with_windows = (
    valid
    .select(
        "*",
        F.lead("monthly_fee_whole", 2, 0).over(w).alias("next_monthly_fee_whole"),
        F.sum("monthly_fee_whole").over(w2).alias("cumulative_monthly_fee_whole"),
    )
)

kept_keys = (
    with_windows
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("plan")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

summary = (
    reconciled
    .withColumn("quarter_start", F.date_trunc("QUARTER", "start_date"))
    .cube("status", "quarter_start")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("next_monthly_fee_whole")).otherwise(0)).alias("cancelled_next_monthly_fee_whole"),
        F.avg("monthly_fee_whole").alias("avg_monthly_fee_whole"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    summary
    .select(
        "status",
        "quarter_start",
        "cancelled_next_monthly_fee_whole",
        "avg_monthly_fee_whole",
        "distinct_customer_ids",
        "grouping_level",
        F.when(F.col("status").rlike("^(EXPIRED|ACTIVE)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("avg_monthly_fee_whole").asc_nulls_last(), "quarter_start")
)
