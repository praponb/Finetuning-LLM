from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

valid = (
    df_sessions
    .filter((~F.col("browser").eqNullSafe(F.lit("Safari"))) & (F.col("started_at") < "2025-03-01"))
    .select("*", F.lower("device").alias("device_lower"))
)

flagged = (
    valid
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

eligible = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") <= F.col("s.started_at")), "left_semi")
)

matched = (
    eligible
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.started_at").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    matched
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "session_id",
        "subscription_id",
        "plan",
        F.col("min_order_amount"),
    )
    .orderBy(F.col("started_at").asc_nulls_first())
)
