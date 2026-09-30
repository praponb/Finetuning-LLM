from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("session_id", "id", "subscription_id")
w5 = Window.partitionBy("country").orderBy("row_num", "id", "subscription_id")
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w7 = Window.partitionBy("browser", "country").orderBy("session_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id"), F.col("s.subscription_id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())

with_window = (
    df_sessions
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & F.col("e.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

flagged = (
    with_window
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .filter(F.col("subscription_id") >= 5)
    .select(
        "id",
        "browser",
        "started_at",
        "subscription_id",
        "plan",
        "monthly_fee",
        "session_id",
        F.max("subscription_id").over(w3).alias("subscription_id_max_7d"),
        F.lead("subscription_id", 2).over(w4).alias("next_subscription_id"),
        F.lead("monthly_fee", 1).over(w5).alias("next_monthly_fee"),
        F.percent_rank().over(w6).alias("monthly_fee_percent_rank"),
        F.last("browser", ignorenulls=True).over(w7).alias("last_known_browser"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.browser",
        "s.subscription_id",
        "s.plan",
        "s.monthly_fee",
        "s.session_id",
        "s.next_monthly_fee",
        F.col("c.page"),
        F.col("c.event_type"),
    )
)

cleaned = (
    latest_match
    .filter(F.col("page").isNotNull())
    .withColumn("plan_length", F.length("plan"))
    .withColumn("browser_filled", F.coalesce("browser", F.lit("OTHER")))
)

remaining_keys = (
    cleaned
    .select("id", "browser")
    .intersect(
        df_sessions
        .filter(~F.col("country").isin(["JP", "TH", "DE"]))
        .select("id", "browser")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "browser",
        "subscription_id",
        "plan",
        "monthly_fee",
        "session_id",
        "page",
        "event_type",
        "plan_length",
        F.when(F.col("plan").rlike("^(basic|premium)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(standard|enterprise)$"), "CORE").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy(F.col("page").asc_nulls_last())
)
