from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("age").desc_nulls_last())

cleaned = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 180) & (F.col("end_date") > F.to_date(F.lit("2023-09-25"))))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.datediff(F.current_date(), "start_date").alias("days_since_start_date"),
    )
)

enriched = (
    cleaned
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") == "Consumer")
    .select(
        "e.id",
        "e.customer_id",
        "e.plan",
        "e.start_date",
        F.col("c.segment"),
        F.col("c.age"),
    )
)

ranked = (
    enriched
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

attributed = (
    ranked
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

with_window = (
    attributed
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.start_date") >= F.col("sub.start_date")) & (F.col("src.start_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan_2"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

result = (
    with_window
    .withColumn("activity_month", F.date_trunc("MONTH", "start_date"))
    .withColumn("subscription_monthly_fee_tier", F.when(F.col("subscription_monthly_fee") >= 228, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["activity_month", "subscription_plan", "subscription_monthly_fee_tier"], ["activity_month", "subscription_plan"], []],
        "activity_month",
        "subscription_plan",
        "subscription_monthly_fee_tier",
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.sort_array(F.collect_set("subscription_plan_2")).alias("subscription_plan_2_list"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.grouping("activity_month").alias("grouping_level"),
    )
)
