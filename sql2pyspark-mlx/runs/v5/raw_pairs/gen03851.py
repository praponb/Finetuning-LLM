from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("session_id").isin([2, 17, 32]))
    .select("*", F.initcap(F.trim("event_type")).alias("event_type_title"))
)

filtered = (
    valid
    .filter((F.col("event_time") >= F.lit("2023-06-08").cast("date")) & F.col("page").isNotNull())
    .withColumn("page_event_type_key", F.concat_ws(",", "page", "event_type"))
    .withColumn("page_length", F.length("page"))
    .withColumn("page_clean", F.coalesce("page", F.lit("UNKNOWN")))
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        F.sum("is_new_session").over(w2).alias("session_no"),
    )
)

scored = (
    islands
    .withColumn("running_count_session_no", F.count("session_no").over(w2))
    .withColumn("running_sum_session_no", F.sum("session_no").over(w2))
    .withColumn("running_min_session_no", F.min("session_no").over(w2))
)

result = scored.select("id", "session_no", "running_min_session_no")
