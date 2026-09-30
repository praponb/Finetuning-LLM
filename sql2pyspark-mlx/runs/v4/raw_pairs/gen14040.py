from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("started_at", "id")
w5 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("customer_id", "browser").orderBy(F.col("started_at").asc_nulls_last(), "id")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

prev_events = latest_match.withColumn("prev_started_at", F.lag("started_at").over(w2))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w3))

flagged = (
    islands
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w4).isNull() | (F.col("started_at") > F.lag("started_at").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands_sessions = flagged.withColumn("island_id", F.sum("is_new_burst").over(w5))

ranked = (
    islands_sessions
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("burst_id")
    .agg(
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("web_event_page")).alias("web_event_page_list"),
    )
    .filter(F.col("desktop_events") < 1)
    .orderBy(F.col("burst_id").desc_nulls_first())
)
