from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id", "event_type").orderBy(F.col("duration_sec").desc())
w2 = Window.partitionBy("page", "event_type").orderBy("duration_sec_whole", "id")
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("event_type").orderBy(F.col("prev_duration_sec").desc(), "id")
w5 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w6 = Window.partitionBy("page").orderBy(F.col("prev_duration_sec").desc(), "id")
w7 = Window.partitionBy("page", "event_type").orderBy(F.col("duration_sec_dense_rank").asc_nulls_first(), "id")
w8 = Window.partitionBy("session_id", "page").orderBy(F.col("row_seq").desc(), "id")
w9 = Window.orderBy("prev_duration_sec", "id")
w10 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_whole").asc_nulls_first())

filtered = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

metrics = (
    filtered
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w))
    .withColumn("prev_duration_sec", F.lag("duration_sec", 2).over(w2))
    .withColumn("last_known_duration_sec_whole", F.last("duration_sec_whole", ignorenulls=True).over(w3))
)

enriched = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w4).alias("prev_duration_sec_bucket"),
        F.row_number().over(w5).alias("row_seq"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.ntile(10).over(w8).alias("row_seq_bucket"),
        F.lead("row_seq", 1).over(w9).alias("next_row_seq"),
    )
)

ranked_web_events = (
    scored
    .withColumn("rn", F.rank().over(w10))
    .filter(F.col("rn") == 1)
)

base = (
    ranked_web_events
    .filter(F.col("row_seq") > 4)
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "page_clean",
        "duration_sec_dense_rank",
        "prev_duration_sec",
        "last_known_duration_sec_whole",
        "row_seq",
        "seq_no",
        "row_seq_bucket",
        "next_row_seq",
        F.floor("seq_no").alias("seq_no_whole"),
        (F.col("row_num") + F.col("last_known_duration_sec_whole")).alias("row_num_plus_last_known_duration_sec_whole"),
        F.coalesce("page", F.lit("N/A")).alias("resolved_page"),
    )
)

result = (
    base
    .select(
        "id",
        "duration_sec",
        "page_clean",
        "duration_sec_dense_rank",
        "prev_duration_sec",
        "last_known_duration_sec_whole",
        "seq_no",
        "row_seq_bucket",
        "next_row_seq",
    )
    .orderBy("prev_duration_sec")
)
