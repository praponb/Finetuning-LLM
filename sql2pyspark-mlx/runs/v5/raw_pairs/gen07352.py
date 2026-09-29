from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("page").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device").orderBy("started_at", "id")
w_ordered = Window.partitionBy("page").orderBy("web_event_duration_sec", "id")
w_part = Window.partitionBy("country").orderBy("web_event_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("country").orderBy(F.col("next_web_event_duration_sec").desc(), "id")
w9 = Window.partitionBy("page", "country").orderBy("row_seq", "id")
w10 = Window.partitionBy("page").orderBy("prev_event_seq", "id")
w11 = Window.partitionBy("page", "device")

latest_match = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page"))
)

scored = (
    latest_match
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "page",
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
        F.row_number().over(w3).alias("row_seq"),
        F.last("page", ignorenulls=True).over(w4).alias("last_known_page"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

latest_match_sessions = (
    scored
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

scored_sessions = (
    latest_match_sessions
    .filter(F.col("row_seq") == 4)
    .select(
        "id",
        "device",
        "country",
        "page",
        "first_known_country",
        "row_seq",
        "last_known_page",
        "event_seq",
        "web_event_duration_sec",
        F.lead("event_seq").over(w_ordered).alias("next_event_seq"),
        F.lag("event_seq").over(w_ordered).alias("prev_event_seq"),
        F.lag("web_event_duration_sec").over(w_ordered).alias("prev_web_event_duration_sec"),
        F.lead("web_event_duration_sec").over(w_ordered).alias("next_web_event_duration_sec"),
        F.row_number().over(w_ordered).alias("row_seq_2"),
        F.sum("event_seq").over(w_part).alias("running_event_seq"),
    )
)

deduped = (
    scored_sessions
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
)

scored_sessions_2 = (
    deduped
    .select(
        "id",
        "device",
        "country",
        "row_seq",
        "last_known_page",
        "web_event_duration_sec",
        "prev_event_seq",
        "prev_web_event_duration_sec",
        "row_seq_2",
        "rn",
        F.ntile(4).over(w9).alias("row_seq_bucket"),
        F.lead("row_seq", 1).over(w10).alias("next_row_seq"),
        F.round((F.col("row_seq") / F.sum("row_seq").over(w11)), 4).alias("row_seq_share"),
    )
)

result = (
    scored_sessions_2
    .select(
        "id",
        "device",
        "country",
        "row_seq",
        "web_event_duration_sec",
        "prev_event_seq",
        "prev_web_event_duration_sec",
        "rn",
        "next_row_seq",
    )
    .orderBy(F.col("web_event_duration_sec").desc())
)
