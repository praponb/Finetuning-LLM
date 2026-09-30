from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("plan", "customer_id")
w6 = Window.partitionBy("status").orderBy("monthly_fee_share", "id")
w_running = Window.orderBy("monthly_fee_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("status").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w10 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee_share").desc_nulls_first(), "id")

joined = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select("s.*", F.col("c.email"), F.col("c.country"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .filter(F.col("dedup_rank") != 1)
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.ntile(3).over(w3).alias("monthly_fee_bucket"),
    )
)

valid = (
    enriched
    .filter((F.col("start_date") > F.lit("2023-08-13").cast("date")) & F.col("country").isin(["UK", "DE"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "email",
        "country",
        "monthly_fee_share",
        F.substring("email", 1, 2).alias("email_prefix"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "status",
        "email",
        "country",
        "monthly_fee_share",
        "email_prefix",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share_2"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share_3"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

with_windows_subscriptions = (
    with_windows
    .select(
        "id",
        "customer_id",
        "status",
        "email",
        "country",
        "monthly_fee_share",
        "email_prefix",
        "monthly_fee_share_2",
        "monthly_fee_share_3",
        "event_seq",
        F.last("country", ignorenulls=True).over(w_running).alias("last_known_country"),
        F.last("country", ignorenulls=True).over(w_grp).alias("last_known_country_2"),
        F.lag("monthly_fee_share_3", 1, 0).over(w9).alias("prev_monthly_fee_share_3"),
    )
)

latest_subscriptions = (
    with_windows_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w10))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest_subscriptions
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("monthly_fee_share")).otherwise(0)).alias("de_monthly_fee_share"),
        F.round(F.avg("monthly_fee_share_3"), 2).alias("avg_monthly_fee_share_3"),
    )
)
