from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("subscription_id").asc_nulls_first())

enriched = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.start_date",
        "a.monthly_fee",
        "a.status",
        F.col("d.email"),
        F.col("d.segment"),
        F.col("d.country"),
    )
)

with_window = (
    enriched
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

base = (
    with_window
    .filter((F.col("start_date") > F.lit("2023-06-26").cast("date")) & (F.col("customer_id") == 30))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "email",
        "segment",
        "country",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        F.coalesce("segment", F.lit("Unknown")).alias("segment_filled"),
    )
)

filtered = (
    base
    .filter((F.col("start_date") >= F.lit("2023-06-29").cast("date")) & (F.col("customer_id") == 5))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "segment",
        "country",
        "subscription_id",
        "subscription_plan",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.upper("country").alias("country_upper"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_window_subscriptions = (
    latest
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee"),
    )
)

with_window_subscriptions_2 = (
    with_window_subscriptions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id_3"),
        F.col("sub.plan").alias("subscription_plan_3"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_window_subscriptions_2
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "segment",
        "subscription_id",
        "subscription_plan",
        "plan_title",
    )
    .orderBy(F.col("subscription_plan").desc())
)
