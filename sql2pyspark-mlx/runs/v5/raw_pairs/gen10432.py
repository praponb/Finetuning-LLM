from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("session_no", "id")

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-15").cast("date")) & (F.col("duration_sec") <= 277))
    .select(
        "*",
        F.when(F.col("page").isin(["/home", "/cart"]), "Core").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

with_prev = filtered.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

with_dims = (
    sessions_web_events
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .filter(F.col("p.customer_id") == 29)
    .select("s.*", F.col("p.browser"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_sessions.alias("d"), F.col("a.session_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.device"), F.col("d.country"))
)

enriched_web_events = (
    enriched
    .select(
        "*",
        F.lag("duration_sec").over(w3).alias("prev_duration_sec"),
        F.lead("duration_sec").over(w3).alias("next_duration_sec"),
        F.lead("session_no").over(w3).alias("next_session_no"),
        F.lag("session_no").over(w3).alias("prev_session_no"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    enriched_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "page_clean",
        "prev_event_time",
        "new_group_flag",
        F.when(F.col("event_type").rlike("^(purchase|search)$"), "GROUP_A").when(F.col("event_type").rlike("^(page_view|click)$"), "PRIORITY").otherwise("GROUP_B").alias("event_type_zone"),
    )
    .orderBy(F.col("new_group_flag").desc_nulls_first(), "id")
)
