from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("plan")
w5 = Window.partitionBy("customer_id").orderBy("plan_avg_monthly_fee")
w_ordered = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)

latest = (
    df_subscriptions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "plan", "start_date", "monthly_fee")
)

with_windows = (
    latest
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.ntile(4).over(w3).alias("monthly_fee_bucket"),
        F.avg("monthly_fee").over(w4).alias("plan_avg_monthly_fee"),
    )
)

latest_subscriptions = (
    with_windows
    .withColumn("dedup_rank", F.rank().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "first_known_monthly_fee",
        "monthly_fee_bucket",
    )
)

enriched = (
    latest_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.email").like("%@gmail.com"))
    .select("e.*", F.col("p.age"), F.col("p.name"))
)

scored = (
    enriched
    .withColumn("running_count_monthly_fee_bucket", F.count("monthly_fee_bucket").over(w_ordered))
    .withColumn("running_max_monthly_fee_bucket", F.max("monthly_fee_bucket").over(w_ordered))
    .withColumn("running_avg_monthly_fee_bucket", F.avg("monthly_fee_bucket").over(w_ordered))
    .withColumn("running_min_monthly_fee_bucket", F.min("monthly_fee_bucket").over(w_ordered))
    .withColumn("monthly_fee_bucket_avg_3", F.avg("monthly_fee_bucket").over(w_ordered_2))
)

valid = (
    scored
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-03-08"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee_bucket",
        "age",
        "running_count_monthly_fee_bucket",
        "running_max_monthly_fee_bucket",
        "running_avg_monthly_fee_bucket",
        "monthly_fee_bucket_avg_3",
        F.upper("plan").alias("plan_upper"),
    )
)

remaining_keys = (
    valid
    .select("customer_id", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("end_date").between("2023-03-24", "2024-08-01"))
        .select("customer_id", "id")
    )
)

kept_rows = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("customer_id")
    .pivot("plan", ["standard", "premium"])
    .agg(F.avg("monthly_fee_bucket_avg_3"))
)
