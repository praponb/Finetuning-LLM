from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").asc_nulls_last(), "id")
w2 = Window.partitionBy("event_type", "session_id").orderBy(F.col("event_time").desc(), "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("session_id").orderBy(F.col("is_new_burst").desc(), "id")
w6 = Window.orderBy("event_time", "id")

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.concat_ws("-", "page", "event_type").alias("page_event_type_key"),
        F.trim("page").alias("clean_page"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_web_events = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

prev_events = ranked_web_events.withColumn("prev_event_time", F.lag("event_time").over(w3))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

enriched = (
    sessionized
    .withColumn("is_new_burst_bucket", F.ntile(3).over(w5))
    .withColumn("next_is_new_burst", F.lead("is_new_burst", 1, 0).over(w6))
)

result = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "is_new_burst",
        "burst_id",
    )
)
