from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("subscription_monthly_fee", "id", "subscription_id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.partitionBy("status").orderBy(F.col("subscription_id").desc(), "id", "subscription_id")
w_part = Window.partitionBy("customer_id").orderBy("start_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("plan", "subscription_plan").orderBy("subscription_monthly_fee_avg_3")
w7 = Window.orderBy("running_monthly_fee", "id", "subscription_id")
w8 = Window.partitionBy("plan").orderBy("monthly_fee", "id", "subscription_id")
w9 = Window.partitionBy("status").orderBy("running_subscription_id")
w10 = Window.partitionBy("status").orderBy(F.col("running_monthly_fee").desc_nulls_last())

metrics = (
    df_subscriptions
    .withColumn("monthly_fee_cume_dist", F.cume_dist().over(w_grp))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
)

with_window = (
    metrics
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

metrics_subscriptions = (
    with_window
    .withColumn("subscription_monthly_fee_avg_3", F.avg("subscription_monthly_fee").over(w3))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("running_subscription_id", F.sum("subscription_id").over(w_part))
)

metrics_subscriptions_2 = (
    metrics_subscriptions
    .select(
        "*",
        F.percent_rank().over(w6).alias("subscription_monthly_fee_avg_3_percent_rank"),
        F.ntile(4).over(w7).alias("running_monthly_fee_bucket"),
    )
)

scored = (
    metrics_subscriptions_2
    .withColumn("monthly_fee_bucket", F.ntile(3).over(w8))
    .withColumn("running_subscription_id_rank", F.rank().over(w9))
)

ranked = (
    scored
    .withColumn("row_num", F.rank().over(w10))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "subscription_id",
        "customer_id",
        F.col("subscription_monthly_fee").alias("SUBSCRIPTION_MONTHLY_FEE"),
        F.col("monthly_fee").alias("MONTHLY_FEE_VAL"),
        "monthly_fee_cume_dist",
    )
    .unpivot(
        ["id", "subscription_id", "customer_id"],
        ["SUBSCRIPTION_MONTHLY_FEE", "MONTHLY_FEE_VAL", "monthly_fee_cume_dist"],
        "kpi",
        "value",
    )
    .filter(F.col("value").isNotNull())
)
