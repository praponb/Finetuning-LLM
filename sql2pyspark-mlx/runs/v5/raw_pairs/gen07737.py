from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id", "subscription_id")
w3 = Window.partitionBy("country").orderBy("started_at", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("country").orderBy("subscription_id", "id", "subscription_id")
w5 = Window.orderBy("started_at", "id", "subscription_id").rowsBetween(-6, Window.currentRow)

valid = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Edge"]))
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.length("browser").alias("browser_length"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("country_filled"),
    )
)

with_window = (
    valid
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    with_window
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "device",
        "started_at",
        "country",
        "browser_length",
        "subscription_id",
        "monthly_fee",
        F.sum("browser_length").over(w).alias("running_browser_length"),
        F.lead("subscription_id", 1).over(w2).alias("next_subscription_id"),
    )
)

flagged = (
    enriched
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w2) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "started_at",
        "country",
        "subscription_id",
        "monthly_fee",
        "running_browser_length",
        F.sum("is_new_session").over(w3).alias("burst_id"),
    )
)

metrics = (
    sessionized
    .select(
        "id",
        "subscription_id",
        "monthly_fee",
        F.lead("running_browser_length", 1, 0).over(w_recent).alias("next_running_browser_length"),
        F.avg("burst_id").over(w5).alias("burst_id_avg_6"),
        F.lag("running_browser_length", 1).over(w2).alias("prev_running_browser_length"),
    )
)

result = (
    metrics
    .select("id", "subscription_id", "burst_id_avg_6", "prev_running_browser_length")
)
