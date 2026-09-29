from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w_hist = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("started_at").desc_nulls_first(), "id")
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id")
w5 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id", "subscription_id_2")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

with_last = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

metrics = (
    with_last
    .withColumn("last_known_web_event_event_type", F.last("web_event_event_type", ignorenulls=True).over(w_hist))
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_hist))
)

deduped = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_window = (
    deduped
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.started_at") >= F.col("sub.start_date")) & (F.col("a.started_at") < F.col("sub.end_date") + F.expr("INTERVAL 30 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

with_prev = with_window.withColumn("prev_started_at", F.lag("started_at").over(w4))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w5))

attributed = (
    sessionized
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

latest = (
    attributed
    .withColumn("latest_rank_2", F.row_number().over(w6))
    .filter(F.col("latest_rank_2") == 1)
    .drop("latest_rank_2")
)

result = (
    latest
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "web_event_event_type",
        "last_known_web_event_event_type",
        "last_known_country",
        F.when(F.col("browser").rlike("^(Safari|Chrome)$"), "PRIORITY").otherwise("CORE").alias("browser_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("browser").asc_nulls_first())
)
