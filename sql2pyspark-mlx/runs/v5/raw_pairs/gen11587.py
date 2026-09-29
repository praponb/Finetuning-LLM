from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec_whole", "id")
w2 = Window.partitionBy("device").orderBy("duration_sec_whole", "id")
w_ordered = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w4 = Window.partitionBy("browser")
w_grp = Window.partitionBy("browser").orderBy("duration_sec_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("browser").orderBy("row_seq", "id")
w7 = Window.partitionBy("browser").orderBy(F.col("duration_sec_rank").asc_nulls_last())
w8 = Window.partitionBy("device", "browser").orderBy(F.col("duration_sec_whole_bucket").asc_nulls_last())

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.when(F.col("event_type") == "purchase", 1).otherwise(0).alias("is_purchase"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.page",
        "s.duration_sec",
        "s.duration_sec_whole",
        F.col("p.browser"),
        F.col("p.device"),
    )
)

enriched = (
    with_dims
    .select(
        "id",
        "browser",
        "device",
        F.ntile(5).over(w).alias("duration_sec_whole_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.rank().over(w_ordered).alias("duration_sec_rank"),
    )
)

enriched_web_events = (
    enriched
    .select(
        "*",
        F.count("duration_sec_whole_bucket").over(w4).alias("browser_count_duration_sec_whole_bucket"),
        F.sum("duration_sec_whole_bucket").over(w_grp).alias("cumulative_duration_sec_whole_bucket"),
        F.lead("duration_sec_whole_bucket", 1).over(w_part).alias("next_duration_sec_whole_bucket"),
        F.dense_rank().over(w7).alias("duration_sec_rank_dense_rank"),
        F.dense_rank().over(w8).alias("duration_sec_whole_bucket_dense_rank"),
    )
)

result = (
    enriched_web_events
    .select(
        "id",
        "browser",
        "device",
        "duration_sec_whole_bucket",
        "browser_count_duration_sec_whole_bucket",
        "cumulative_duration_sec_whole_bucket",
        "next_duration_sec_whole_bucket",
        "duration_sec_rank_dense_rank",
    )
    .orderBy(F.col("duration_sec_whole_bucket").desc(), "browser")
)
