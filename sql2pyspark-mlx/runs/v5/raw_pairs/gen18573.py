from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at_date").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("device").orderBy(F.col("started_at_date").desc(), "id")

cleaned = (
    df_sessions
    .filter((~F.col("browser").eqNullSafe(F.lit("Chrome"))) & F.col("browser").isin(["Firefox", "Safari"]))
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        F.to_date("started_at").alias("started_at_date"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.duration_sec"))
)

eligible = (
    with_latest
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.event_time") >= F.col("s.started_at_date")), "left_anti")
)

deduped_sessions = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

attributed = (
    deduped_sessions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & (F.col("s.started_at_date") >= F.col("sub.start_date")) & (F.col("s.started_at_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

kept = (
    attributed
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") >= F.col("s.started_at_date")), "left_semi")
)

result = (
    kept
    .groupBy("country", "device")
    .pivot("event_type", ["purchase", "add_to_cart"])
    .agg(F.max("duration_sec").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
