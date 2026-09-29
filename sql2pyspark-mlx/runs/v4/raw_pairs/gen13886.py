from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        F.substring("status", 1, 4).alias("status_prefix"),
    )
)

result = (
    base
    .groupBy("customer_id")
    .agg(
        F.max(F.when(F.col("plan") == "premium", F.col("monthly_fee"))).alias("premium_max_monthly_fee"),
        F.count(F.when(F.col("plan") == "standard", 1)).alias("standard_rows"),
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
    )
    .orderBy(F.col("premium_max_monthly_fee").desc())
)
