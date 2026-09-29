from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

eligible = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < F.col("s.started_at")), "left_anti")
)

kept_keys = (
    eligible
    .select("country", "browser")
    .subtract(
        df_sessions
        .filter(F.to_date("started_at") < "2024-12-09")
        .select("country", "browser")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.browser") == F.col("k.browser")), "inner")
    .select("s.*")
)

joined = (
    reconciled
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select(
        "src.customer_id",
        "src.device",
        "src.browser",
        "src.started_at",
        "src.is_new_session",
        F.col("d.city"),
        F.col("d.age"),
    )
)

result = (
    joined
    .withColumn("is_new_session_tier", F.when(F.col("is_new_session") >= 1, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("week", "started_at"))
    .withColumn("activity_month", F.date_trunc("month", "started_at"))
    .rollup("is_new_session_tier", "week_start", "activity_month")
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
        F.min("started_at").alias("started_at"),
        F.count_if(F.col("city") == "London").alias("london_events"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.grouping("is_new_session_tier").alias("grouping_id"),
    )
    .filter(F.col("mobile_count") >= 1)
)
