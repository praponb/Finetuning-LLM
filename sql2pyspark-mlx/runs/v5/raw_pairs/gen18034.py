from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("session_id").orderBy("session_no", "id")
w4 = Window.partitionBy("event_type").orderBy(F.col("prev_event_time").desc(), "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("event_time").asc_nulls_first(), "id")
w6 = Window.partitionBy("event_type").orderBy("prev_event_time", "id")
w7 = Window.partitionBy("event_type").orderBy("prev_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_prev = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("is_new_session").over(w2))

scored = (
    islands
    .filter(F.col("page").isin(["/cart", "/search"]))
    .select(
        "id",
        "event_type",
        "event_time",
        "prev_event_time",
        "session_no",
        F.lag("is_new_session").over(w_recent).alias("prev_is_new_session"),
        F.lead("is_new_session").over(w_recent).alias("next_is_new_session"),
        F.lag("session_no").over(w_recent).alias("prev_session_no"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

ranked_web_events = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "event_type",
        "prev_event_time",
        "session_no",
        "prev_is_new_session",
        "prev_session_no",
        "row_seq",
    )
)

with_gaps_2 = (
    ranked_web_events
    .withColumn("session_start_flag", F.when(F.lag("prev_event_time").over(w6).isNull() | (F.col("prev_event_time") > F.lag("prev_event_time").over(w6) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps_2.withColumn("session_id", F.sum("session_start_flag").over(w7))

grouped = (
    sessions_web_events
    .cube("prev_session_no")
    .agg(
        F.count("*").alias("num_rows"),
        F.max("prev_event_time").alias("last_seen"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.min("prev_event_time").alias("first_prev_event_time"),
    )
)

result = (
    grouped
    .select(
        "prev_session_no",
        "last_seen",
        "row_count",
        "purchase_events",
        "first_prev_event_time",
    )
)
