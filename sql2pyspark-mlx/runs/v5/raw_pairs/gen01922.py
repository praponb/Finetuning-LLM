from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("plan").orderBy("end_date", "id")
w3 = Window.partitionBy("plan")
w4 = Window.orderBy("start_date", "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-04-24"))) & (F.col("plan") == "standard"))
    .withColumn("status_lower", F.lower("status"))
)

enriched = (
    cleaned
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "status_lower",
        F.sum("monthly_fee").over(w_hist).alias("running_monthly_fee"),
        F.lag("monthly_fee", 1).over(w_ordered).alias("prev_monthly_fee"),
        F.avg("monthly_fee").over(w3).alias("plan_avg_monthly_fee"),
        F.lag("monthly_fee", 1).over(w4).alias("prev_monthly_fee_2"),
    )
)

cleaned_subscriptions = (
    enriched
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-11-09"))) & (F.col("end_date") <= "2025-04-01"))
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        "running_monthly_fee",
        "prev_monthly_fee",
        "prev_monthly_fee_2",
        F.round("running_monthly_fee", 0).alias("running_monthly_fee_rounded"),
        F.substring("status", 1, 3).alias("status_prefix"),
    )
)

cleaned_subscriptions_2 = (
    cleaned_subscriptions
    .filter(F.col("end_date") > F.to_date(F.lit("2023-06-21")))
    .select(
        "id",
        "customer_id",
        "status",
        "running_monthly_fee",
        "prev_monthly_fee",
        "prev_monthly_fee_2",
        "running_monthly_fee_rounded",
        "status_prefix",
        (F.col("running_monthly_fee") + F.col("running_monthly_fee_rounded")).alias("running_monthly_fee_plus_running_monthly_fee_rounded"),
        F.coalesce("running_monthly_fee_rounded", F.lit(0)).alias("running_monthly_fee_rounded_filled"),
    )
)

enriched_subscriptions = (
    cleaned_subscriptions_2
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.country") != "UK")
    .select("a.*", F.col("b.age"), F.col("b.city"))
)

result = (
    enriched_subscriptions
    .groupBy(
        F.when(F.col("prev_monthly_fee") >= 219, "PREMIUM").otherwise("STANDARD").alias("prev_monthly_fee_tier"),
        F.when(F.col("running_monthly_fee_plus_running_monthly_fee_rounded") >= 89, "PREMIUM").otherwise("STANDARD").alias("running_monthly_fee_plus_running_monthly_fee_rounded_tier"),
        F.when(F.col("prev_monthly_fee_2") >= 205, "LARGE").otherwise("SMALL").alias("prev_monthly_fee_2_tier"),
    )
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("prev_monthly_fee_2")).otherwise(0)).alias("cancelled_prev_monthly_fee_2"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.avg("running_monthly_fee_rounded_filled"), 2).alias("avg_running_monthly_fee_rounded_filled"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_rows"),
    )
    .orderBy("prev_monthly_fee_tier")
)
