from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status")
w2 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("subscription_monthly_fee").asc_nulls_last())
w4 = Window.partitionBy("customer_id").orderBy("subscription_id", "id", "subscription_id")
w5 = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-5, -1)
w6 = Window.partitionBy("customer_id", "subscription_plan")
w7 = Window.partitionBy("subscription_plan", "status").orderBy("subscription_monthly_fee_percent_rank", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status")
w9 = Window.orderBy("subscription_id_avg_5_prior", "id", "subscription_id")
w_hist = Window.orderBy("end_date", "id", "subscription_id")

metrics = (
    df_subscriptions
    .withColumn("customer_id_stddev_samp_monthly_fee", F.stddev_samp("monthly_fee").over(w))
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w2))
)

with_window = (
    metrics
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

with_windows = (
    with_window
    .select(
        "*",
        F.percent_rank().over(w3).alias("subscription_monthly_fee_percent_rank"),
        F.lag("first_known_monthly_fee", 1).over(w4).alias("prev_first_known_monthly_fee"),
        F.avg("subscription_id").over(w5).alias("subscription_id_avg_5_prior"),
        F.avg("subscription_monthly_fee").over(w6).alias("customer_id_avg_subscription_monthly_fee"),
    )
)

result = (
    with_windows
    .select(
        "*",
        F.first("first_known_monthly_fee", ignorenulls=True).over(w7).alias("first_known_first_known_monthly_fee"),
        F.min("subscription_monthly_fee_percent_rank").over(w8).alias("status_min_subscription_monthly_fee_percent_rank"),
        F.lead("first_known_monthly_fee", 1).over(w9).alias("next_first_known_monthly_fee"),
        F.lead("subscription_monthly_fee", 1).over(w_hist).alias("next_subscription_monthly_fee"),
    )
)
