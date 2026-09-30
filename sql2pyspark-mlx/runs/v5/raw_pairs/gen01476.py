from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.orderBy(F.col("prev_session_no").desc_nulls_first())
w5 = Window.partitionBy("page").orderBy("prev_duration_sec", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("first_known_duration_sec", "id")

valid = (
    df_web_events
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-05-27"))) & F.col("page").isNotNull())
    .select(
        "*",
        F.lower("event_type").alias("event_type_lower"),
        F.concat("page", F.lit(" - "), "event_type").alias("page_event_type"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

enriched = (
    islands
    .filter(F.col("event_type") == "click")
    .select(
        "id",
        "page",
        "event_type_lower",
        "new_group_flag",
        F.lag("session_no").over(w3).alias("prev_session_no"),
        F.lead("session_no").over(w3).alias("next_session_no"),
        F.lead("duration_sec").over(w3).alias("next_duration_sec"),
        F.lag("duration_sec").over(w3).alias("prev_duration_sec"),
        F.row_number().over(w3).alias("seq_no"),
        F.first("duration_sec", ignorenulls=True).over(w2).alias("first_known_duration_sec"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "next_session_no",
        "first_known_duration_sec",
        F.cume_dist().over(w4).alias("prev_session_no_cume_dist"),
        F.avg("first_known_duration_sec").over(w5).alias("first_known_duration_sec_avg_6"),
        F.ntile(5).over(w6).alias("first_known_duration_sec_bucket"),
    )
)

result = (
    metrics
    .select("id", "prev_session_no_cume_dist", "first_known_duration_sec_bucket")
    .orderBy(F.col("id").desc())
)
