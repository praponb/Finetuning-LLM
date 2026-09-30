from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("event_type").orderBy("is_new_session", "id").rowsBetween(-2, Window.currentRow)
w_part = Window.partitionBy("event_type").orderBy("last3_count_is_new_session", "id")
w_hist = Window.partitionBy("session_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("session_id").orderBy("running_min_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") >= "2024-10-01")
    .withColumn("page_group", F.when(F.col("page").isin(["/search", "/cart"]), "Priority").otherwise("Other"))
    .withColumn("page_filled", F.coalesce("page", F.lit("OTHER")))
)

gaps = (
    base
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("session_id_2", F.sum("is_new_session").over(w2))

scored = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "page_group",
        "is_new_session",
        F.min("is_new_session").over(w_grp).alias("last3_min_is_new_session"),
        F.count("is_new_session").over(w_grp).alias("last3_count_is_new_session"),
        F.max("is_new_session").over(w_grp).alias("last3_max_is_new_session"),
        F.sum("is_new_session").over(w_grp).alias("last3_sum_is_new_session"),
    )
)

enriched = (
    scored
    .filter(F.col("last3_max_is_new_session") < 0.73)
    .select(
        "id",
        "session_id",
        "event_type",
        "page_group",
        "last3_count_is_new_session",
        F.lag("last3_count_is_new_session").over(w_part).alias("prev_last3_count_is_new_session"),
        F.lead("last3_sum_is_new_session").over(w_part).alias("next_last3_sum_is_new_session"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "session_id",
        "event_type",
        "page_group",
        "last3_count_is_new_session",
        "prev_last3_count_is_new_session",
        "next_last3_sum_is_new_session",
        "seq_no",
        F.max("seq_no").over(w_hist).alias("running_max_seq_no"),
        F.min("seq_no").over(w_hist).alias("running_min_seq_no"),
        F.sum("seq_no").over(w_hist).alias("running_sum_seq_no"),
    )
)

scored_web_events = (
    metrics
    .withColumn("running_sum_prev_last3_count_is_new_session", F.sum("prev_last3_count_is_new_session").over(w6))
    .withColumn("running_min_prev_last3_count_is_new_session", F.min("prev_last3_count_is_new_session").over(w6))
    .withColumn("running_count_prev_last3_count_is_new_session", F.count("prev_last3_count_is_new_session").over(w6))
)

result = (
    scored_web_events
    .select(
        "id",
        "event_type",
        "page_group",
        "prev_last3_count_is_new_session",
        "next_last3_sum_is_new_session",
        "seq_no",
        "running_max_seq_no",
        "running_min_seq_no",
        "running_sum_seq_no",
        F.when(F.col("event_type").rlike("^(click|search)$"), "CORE").otherwise("PRIORITY").alias("event_type_zone"),
    )
    .orderBy("running_min_seq_no", F.col("running_sum_seq_no").desc())
)
