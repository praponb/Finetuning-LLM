from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id")
w2 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w_recent = Window.partitionBy("status").orderBy("status_count_monthly_fee", "id", "subscription_id")
w4 = Window.orderBy("monthly_fee_rank", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("customer_id").orderBy("start_date_plus_30d", "id", "subscription_id")
w6 = Window.orderBy("status_count_monthly_fee", "id", "subscription_id")
w7 = Window.partitionBy("country")
w8 = Window.partitionBy("subscription_plan", "plan")
w9 = Window.partitionBy("status").orderBy("end_date", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("end_date") > F.to_date(F.lit("2023-12-11"))) & F.col("customer_id").isNotNull() & F.month("start_date").isin([2, 3, 12]))
    .withColumn("start_date_plus_30d", F.date_add("start_date", 30))
)

with_dims = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.segment"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.count("monthly_fee").over(w).alias("status_count_monthly_fee"),
        F.rank().over(w2).alias("monthly_fee_rank"),
    )
)

attributed = (
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

metrics = (
    attributed
    .withColumn("status_count_monthly_fee_bucket", F.ntile(10).over(w_recent))
    .withColumn("first_known_status_count_monthly_fee", F.first("status_count_monthly_fee", ignorenulls=True).over(w4))
    .withColumn("prev_monthly_fee_rank", F.lag("monthly_fee_rank", 2).over(w_part))
    .withColumn("seq_no", F.row_number().over(w6))
)

enriched_subscriptions = (
    metrics
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"))
)

result = (
    enriched_subscriptions
    .select(
        "*",
        F.round((F.col("prev_monthly_fee_rank") / F.sum("prev_monthly_fee_rank").over(w7)), 4).alias("prev_monthly_fee_rank_share"),
        F.round((F.col("status_count_monthly_fee") / F.sum("status_count_monthly_fee").over(w8)), 4).alias("status_count_monthly_fee_share"),
        F.sum("monthly_fee").over(w9).alias("running_monthly_fee"),
    )
)
