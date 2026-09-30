from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("island_id").desc())
w_grp = Window.orderBy("island_id_percent_rank", "id")
w_part = Window.orderBy(F.col("island_id_percent_rank").desc(), "id")
w7 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("island_id_percent_rank_bucket", "id")

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("island_id"),
    )
)

enriched = (
    sessions_web_events
    .select(
        "id",
        "event_time",
        F.first("page", ignorenulls=True).over(w3).alias("first_known_page"),
        F.percent_rank().over(w4).alias("island_id_percent_rank"),
    )
)

scored = (
    enriched
    .filter(F.hour("event_time").between(12, 15))
    .select(
        "id",
        "first_known_page",
        "island_id_percent_rank",
        F.lead("island_id_percent_rank").over(w_grp).alias("next_island_id_percent_rank"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.ntile(3).over(w_part).alias("island_id_percent_rank_bucket"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.lead("next_island_id_percent_rank", 1, 0).over(w_grp).alias("next_next_island_id_percent_rank"),
        F.sum("island_id_percent_rank_bucket").over(w7).alias("cumulative_island_id_percent_rank_bucket"),
        F.row_number().over(w8).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "island_id_percent_rank",
        "next_island_id_percent_rank",
        "row_seq",
        "island_id_percent_rank_bucket",
        "cumulative_island_id_percent_rank_bucket",
    )
)
