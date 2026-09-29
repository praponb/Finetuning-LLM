from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("browser").orderBy("started_at", "id")
w5 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

valid = (
    df_sessions
    .filter(F.col("country").isin(["JP", "UK"]))
    .select("*", F.trim("device").alias("clean_device"))
)

with_prev = valid.withColumn("prev_started_at", F.lag("started_at").over(w))

gaps = (
    with_prev
    .withColumn("is_new_burst", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_last = (
    islands
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.page").alias("web_event_page"),
        F.col("c.event_type").alias("web_event_event_type"),
    )
)

joined = (
    with_last
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.email"), F.col("d.country").alias("customer_country"))
)

flagged = (
    joined
    .withColumn("is_new_burst_2", F.when(F.col("started_at") - F.lag("started_at").over(w4) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands_sessions = flagged.withColumn("island_id", F.sum("is_new_burst_2").over(w5))

matched_keys = (
    islands_sessions
    .select("customer_id", "country")
    .exceptAll(
        df_sessions
        .filter(F.col("country").isin(["JP", "US", "UK"]))
        .select("customer_id", "country")
    )
)

reconciled = (
    islands_sessions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    reconciled
    .select("browser", "device")
    .intersectAll(
        df_sessions
        .filter(F.col("started_at") >= "2023-05-01")
        .select("browser", "device")
    )
)

reconciled_sessions = (
    reconciled
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.browser") == F.col("k.browser")) & (F.col("s.device") == F.col("k.device")), "inner")
    .select("s.*")
)

result = (
    reconciled_sessions
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "clean_device",
        "prev_started_at",
        "web_event_page",
        "web_event_event_type",
        "email",
        F.when(F.col("country").rlike("^(US|JP)$"), "GROUP_B").when(F.col("country").rlike("^(TH|SG)$"), "CORE").otherwise("PRIORITY").alias("country_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
)
