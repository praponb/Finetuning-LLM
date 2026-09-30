from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("event_type").orderBy("started_at", "id")
w4 = Window.partitionBy("event_type").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("web_event_page", "customer_id").orderBy(F.col("started_at").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w6 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("web_event_page").orderBy("started_at", "id")
w8 = Window.partitionBy("device").orderBy("is_new_burst", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("country").orderBy("session_id", "id")

filtered = (
    df_sessions
    .filter(F.to_date("started_at") >= "2025-04-01")
    .withColumn("country_prefix", F.substring("country", 1, 3))
)

joined = (
    filtered
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.country").alias("customer_country"),
        F.col("d.name"),
        F.col("d.email"),
    )
)

latest_match = (
    joined
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

with_last = (
    latest_match
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type"))
)

gaps = (
    with_last
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w3).isNull() | (F.col("started_at") > F.lag("started_at").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_id", F.sum("is_new_burst").over(w4))

scored = (
    islands
    .select(
        "*",
        F.sum("session_id").over(w5).alias("session_id_sum_7d"),
        F.count("session_id").over(w6).alias("session_id_count_30d"),
        F.lag("session_id", 1).over(w7).alias("prev_session_id"),
        F.avg("is_new_burst").over(w8).alias("is_new_burst_avg_5_prior"),
        F.lead("session_id", 1).over(w9).alias("next_session_id"),
    )
)

grouped = (
    scored
    .groupBy(
        F.when(F.col("session_id_sum_7d") >= 45, "PREMIUM").otherwise("STANDARD").alias("session_id_sum_7d_tier"),
        F.when(F.col("session_id") >= 7, "LARGE").otherwise("SMALL").alias("session_id_tier"),
    )
    .agg(
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.sum(F.when(F.col("customer_country") == "DE", F.col("prev_session_id")).otherwise(0)).alias("de_prev_session_id"),
        F.percentile_approx("next_session_id", [0.25, 0.5, 0.75]).alias("next_session_id_quartiles"),
    )
    .filter(F.col("avg_is_new_burst") < 0)
)

result = (
    grouped
    .select("session_id_sum_7d_tier", "session_id_tier")
    .orderBy(F.col("session_id_sum_7d_tier").desc())
)
