from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee_whole").desc_nulls_last(), "id", "subscription_id")
w2 = Window.partitionBy("plan")
w3 = Window.orderBy("subscription_id", "id", "subscription_id").rowsBetween(-3, -1)
w4 = Window.partitionBy("city").orderBy("subscription_id", "id", "subscription_id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_whole").desc_nulls_last(), "id", "subscription_id")
w6 = Window.partitionBy("customer_id").orderBy("prev_monthly_fee_whole", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("customer_id").orderBy("start_date", "id", "subscription_id")

filtered = (
    df_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-10-03")))
    .withColumn("clean_plan", F.trim("plan"))
    .withColumn("monthly_fee_whole", F.floor("monthly_fee"))
)

attributed = (
    filtered
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.start_date") >= F.col("sub.start_date")) & (F.col("a.start_date") <= F.date_add(F.col("sub.end_date"), 3)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

deduped = (
    attributed
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    deduped
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.city") != "Bangkok")
    .select(
        "a.id",
        "a.customer_id",
        "a.plan",
        "a.start_date",
        "a.end_date",
        "a.status",
        "a.monthly_fee_whole",
        "a.subscription_id",
        F.col("p.credit_limit"),
        F.col("p.segment"),
        F.col("p.city"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "customer_id",
        "start_date",
        "status",
        "monthly_fee_whole",
        "subscription_id",
        "city",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.avg("monthly_fee_whole").over(w3).alias("monthly_fee_whole_avg_3_prior"),
        F.lag("monthly_fee_whole", 2).over(w4).alias("prev_monthly_fee_whole"),
    )
)

deduped_subscriptions = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee_whole",
        "subscription_id",
        "monthly_fee_whole_avg_3_prior",
        "prev_monthly_fee_whole",
    )
)

enriched_subscriptions = (
    deduped_subscriptions
    .withColumn("running_count_monthly_fee_whole", F.count("monthly_fee_whole").over(w6))
    .withColumn("running_sum_monthly_fee_whole", F.sum("monthly_fee_whole").over(w6))
    .withColumn("running_min_monthly_fee_whole", F.min("monthly_fee_whole").over(w6))
    .withColumn("running_max_monthly_fee_whole", F.max("monthly_fee_whole").over(w6))
    .withColumn("next_subscription_id", F.lead("subscription_id", 1).over(w7))
)

result = (
    enriched_subscriptions
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.segment").alias("segment"))
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.monthly_fee_whole_avg_3_prior")).alias("min_monthly_fee_whole_avg_3_prior"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("min_monthly_fee_whole_avg_3_prior")
)
