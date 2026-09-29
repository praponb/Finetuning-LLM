from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("subscription_id", "id", "subscription_id")

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.to_timestamp(F.lit("2023-11-18 04:00:00")))
    .withColumn("country_prefix", F.substring("country", 1, 2))
    .withColumn("device_upper", F.upper("device"))
)

eligible = (
    valid
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") <= F.col("s.started_at")), "left_anti")
)

with_window = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at") >= F.col("sub.start_date")) & (F.col("s.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

latest = (
    with_window
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    latest
    .filter((F.col("started_at") > F.to_date(F.lit("2023-12-25"))) & F.col("browser").isin(["Chrome", "Firefox", "Edge"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country_prefix",
        "device_upper",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.initcap(F.trim("browser")).alias("browser_title"),
        F.coalesce("browser", F.lit("N/A")).alias("browser_clean"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "started_at",
        "country_prefix",
        "device_upper",
        "subscription_id",
        "browser_clean",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
