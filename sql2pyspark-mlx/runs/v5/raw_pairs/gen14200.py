from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("running_count_duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy("running_sum_duration_sec", "id")
w4 = Window.partitionBy("page")
w5 = Window.partitionBy("page", "country").orderBy("running_sum_duration_sec", "id")
w6 = Window.partitionBy("page", "session_id").orderBy("running_sum_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("session_id").orderBy(F.col("running_count_duration_sec").desc())

scored = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        F.sum("duration_sec").over(w_part).alias("running_sum_duration_sec"),
        F.min("duration_sec").over(w_part).alias("running_min_duration_sec"),
        F.count("duration_sec").over(w_part).alias("running_count_duration_sec"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

joined = (
    deduped
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.page",
        "e.running_sum_duration_sec",
        "e.running_count_duration_sec",
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.country"),
    )
)

enriched = (
    joined
    .filter(F.col("running_sum_duration_sec").isNotNull())
    .select(
        "*",
        F.row_number().over(w3).alias("event_seq"),
        F.min("running_count_duration_sec").over(w4).alias("page_min_running_count_duration_sec"),
        F.ntile(3).over(w5).alias("running_sum_duration_sec_bucket"),
        F.sum("running_sum_duration_sec").over(w6).alias("running_running_sum_duration_sec"),
        F.rank().over(w7).alias("running_count_duration_sec_rank"),
    )
)

filtered = (
    enriched
    .filter(F.col("running_count_duration_sec_rank") >= 4)
    .select(
        "id",
        "page",
        "browser",
        "country",
        "page_min_running_count_duration_sec",
        "running_sum_duration_sec_bucket",
        "running_running_sum_duration_sec",
        "running_count_duration_sec_rank",
        F.round("page_min_running_count_duration_sec", 2).alias("page_min_running_count_duration_sec_rounded"),
        F.greatest("page_min_running_count_duration_sec", "running_count_duration_sec").alias("greatest_page_min_running_count_duration_sec_running_count_duration_sec"),
    )
)

result = (
    filtered
    .select(
        "id",
        "browser",
        "country",
        "page_min_running_count_duration_sec",
        "running_running_sum_duration_sec",
        "page_min_running_count_duration_sec_rounded",
        "greatest_page_min_running_count_duration_sec_running_count_duration_sec",
    )
)
