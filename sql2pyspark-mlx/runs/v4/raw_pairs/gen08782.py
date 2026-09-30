from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy(F.col("is_basic").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("subscription_id").desc_nulls_first(), "id", "subscription_id", "subscription_id_2", "subscription_id_3")

filtered = (
    df_subscriptions
    .filter(F.col("status") == "ACTIVE")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.when(F.col("plan") == "basic", 1).otherwise(0).alias("is_basic"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

matched = (
    latest
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

attributed = (
    matched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date") >= F.col("sub.start_date")) & (F.col("src.start_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_2"),
    )
)

matched_subscriptions = (
    attributed
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id_3"),
        F.col("sub.plan").alias("subscription_plan_3"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee_3"),
    )
)

filtered_subscriptions = (
    matched_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-05-20").cast("date")) & F.col("subscription_plan_3").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "is_basic",
        "rank_in_group",
        "subscription_id",
        "subscription_plan",
        "subscription_monthly_fee",
        "subscription_id_2",
        "subscription_plan_2",
        "subscription_id_3",
        "subscription_monthly_fee_3",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.coalesce("subscription_plan_3", F.lit("N/A")).alias("subscription_plan_3_filled"),
        F.coalesce("subscription_plan_2", F.lit("UNKNOWN")).alias("resolved_subscription_plan_2"),
    )
)

deduped = (
    filtered_subscriptions
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

grouped = (
    deduped
    .withColumn("is_basic_tier", F.when(F.col("is_basic") >= 62, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("year", "start_date"))
    .groupingSets(
        [["is_basic_tier", "year_start", "subscription_plan_2"], ["is_basic_tier", "year_start"], []],
        "is_basic_tier",
        "year_start",
        "subscription_plan_2",
    )
    .agg(
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.count_if(F.col("subscription_plan") == "basic").alias("basic_events"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_count"),
    )
)

result = (
    grouped
    .select("is_basic_tier", "subscription_plan_2", "basic_events")
    .orderBy(F.col("basic_events").asc_nulls_last())
)
