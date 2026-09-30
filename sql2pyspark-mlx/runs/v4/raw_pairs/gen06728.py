from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("plan").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("browser", "customer_country")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") <= F.col("s.started_at")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"), F.col("c.page").alias("web_event_page"))
)

matched = (
    latest_match
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.started_at") >= F.col("sub.start_date")) & (F.col("e.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

enriched = (
    matched
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.*",
        F.col("c.email"),
        F.col("c.country").alias("customer_country"),
        F.col("c.age"),
    )
)

enriched_sessions = (
    enriched
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w2))
    .withColumn("browser_avg_age", F.avg("age").over(w3))
)

remaining_keys = (
    enriched_sessions
    .select("browser")
    .subtract(
        df_sessions
        .filter(F.col("customer_id").isin([12, 27, 37]))
        .select("browser")
    )
)

verified = (
    enriched_sessions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
    )
    .orderBy(F.col("jp_events").asc_nulls_first())
)
