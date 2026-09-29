from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("customer_id").orderBy("event_time", "id")
w3 = Window.partitionBy("customer_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-06-16 20:00:00").cast("timestamp")) & (~F.col("country").eqNullSafe(F.lit("UK"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("country").alias("country_length"),
        F.upper("browser").alias("browser_upper"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"), F.col("c.event_time"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.session_id").isin([19, 37])), "left_semi")
)

gaps = (
    not_flagged
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w2).isNull() | (F.col("event_time") > F.lag("event_time").over(w2) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("new_group_flag").over(w3))

joined = (
    sessions_sessions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "inner")
    .select(
        "s.*",
        F.col("p.email"),
        F.col("p.segment"),
        F.col("p.country").alias("customer_country"),
    )
)

result = (
    joined
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country_length",
        "browser_upper",
        "page",
        "event_time",
        F.when(F.col("browser").rlike("^(Safari|Chrome)$"), "EXTENDED").when(F.col("browser").rlike("^(Firefox|Edge)$"), "GROUP_A").otherwise("GROUP_B").alias("browser_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("browser_zone").asc_nulls_last())
)
