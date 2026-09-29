from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy("subscription_monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("start_date", "id", "subscription_id")
w4 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id")
w5 = Window.partitionBy("status").orderBy("start_date", "id", "subscription_id")

ranked = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base = (
    ranked
    .filter(F.month("end_date").isin([2, 3, 10]))
    .select(
        "*",
        F.upper("status").alias("status_upper"),
        F.substring("plan", 1, 2).alias("plan_prefix"),
    )
)

attributed = (
    base
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.end_date") >= F.col("sub.start_date")) & (F.col("s.end_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

scored = (
    attributed
    .withColumn("cumulative_subscription_monthly_fee", F.sum("subscription_monthly_fee").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("row_seq_2", F.row_number().over(w5))
)

filtered = (
    scored
    .filter(F.col("start_date") >= F.lit("2023-12-09").cast("date"))
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "rn",
        "status_upper",
        "plan_prefix",
        "subscription_id",
        "subscription_plan",
        "cumulative_subscription_monthly_fee",
        "event_seq",
        "row_seq",
        "row_seq_2",
        F.when(F.col("event_seq") >= 7, "High").when(F.col("event_seq") >= 4, "Medium").otherwise("Low").alias("event_seq_tier"),
    )
)

filtered_subscriptions = (
    filtered
    .filter(F.col("rn") >= 1)
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status",
        "rn",
        "status_upper",
        "subscription_id",
        "subscription_plan",
        "cumulative_subscription_monthly_fee",
        "row_seq",
        "row_seq_2",
        "event_seq_tier",
        F.least("row_seq_2", "event_seq").alias("least_row_seq_2_event_seq"),
        F.round((F.col("cumulative_subscription_monthly_fee") / F.col("row_seq")), 2).alias("cumulative_subscription_monthly_fee_per_row_seq"),
    )
)

filtered_subscriptions_2 = (
    filtered_subscriptions
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-10-17")))
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status_upper",
        "subscription_id",
        "subscription_plan",
        "cumulative_subscription_monthly_fee",
        "row_seq",
        "row_seq_2",
        "event_seq_tier",
        "least_row_seq_2_event_seq",
        "cumulative_subscription_monthly_fee_per_row_seq",
        F.date_trunc("YEAR", "start_date").alias("start_date_period"),
    )
)

result = (
    filtered_subscriptions_2
    .select(
        "id",
        "start_date",
        "monthly_fee",
        "status_upper",
        "subscription_id",
        "subscription_plan",
        "cumulative_subscription_monthly_fee",
        "row_seq_2",
        "event_seq_tier",
    )
    .orderBy(
        F.col("cumulative_subscription_monthly_fee").desc(),
        F.col("row_seq_2").desc(),
    )
)
