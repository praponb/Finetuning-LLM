from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w2 = Window.partitionBy("session_id").orderBy(F.col("event_time").desc_nulls_last(), "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("session_no", "id")
w6 = Window.partitionBy("page").orderBy("session_no", "id")

ranked = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked_web_events = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

flagged = (
    ranked_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "latest_rank",
        F.sum("is_new_burst").over(w4).alias("session_no"),
    )
)

valid = (
    islands
    .filter(F.col("event_time") > F.lit("2023-06-18").cast("date"))
    .select(
        "id",
        "event_type",
        "page",
        "latest_rank",
        "session_no",
        F.upper("event_type").alias("event_type_upper"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

enriched = (
    valid
    .filter(F.col("page") != "/checkout")
    .select(
        "id",
        "event_type",
        "page",
        "event_type_upper",
        "page_filled",
        F.lead("session_no").over(w5).alias("next_session_no"),
        F.row_number().over(w5).alias("event_seq"),
        F.lead("session_no", 1).over(w6).alias("next_session_no_2"),
    )
)

result = (
    enriched
    .select("id", "event_type", "event_type_upper", "page_filled", "next_session_no")
    .orderBy(F.col("event_type").desc())
)
