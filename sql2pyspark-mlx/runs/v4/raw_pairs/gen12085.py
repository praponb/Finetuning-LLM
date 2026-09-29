from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy(F.col("duration_sec_whole").desc_nulls_first(), "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id")
w3 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.substring("event_type", 1, 2).alias("event_type_prefix"),
        F.ceil("duration_sec").alias("duration_sec_whole"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w2) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

result = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_type_prefix",
        "duration_sec_whole",
        "island_id",
        F.when(F.col("event_type").rlike("^(purchase|click)$"), "CORE").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "GROUP_B").otherwise("EXTENDED").alias("event_type_zone"),
    )
)
