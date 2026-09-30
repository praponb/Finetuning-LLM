from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")

deduped = (
    df_web_events
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    deduped
    .select("id", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") >= "2024-05-03")
        .select("id", "session_id")
    )
)

reconciled = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter((F.col("event_time") >= F.lit("2024-01-19 12:00:00").cast("timestamp")) & F.col("page").isNotNull() & (F.col("event_time") < "2023-08-01"))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "rank_in_group",
        F.floor("duration_sec").alias("duration_sec_whole"),
    )
)

result = (
    filtered
    .groupBy(
        F.date_trunc("WEEK", "event_time").alias("week_start"),
        F.when(F.col("duration_sec_whole") >= 67, "HIGH").otherwise("LOW").alias("duration_sec_whole_tier"),
    )
    .agg(
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec_whole")).otherwise(0)).alias("checkout_duration_sec_whole"),
        F.size(F.collect_set("event_type")).alias("num_event_types_2"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec_whole"))).alias("purchase_total_duration_sec_whole"),
    )
)
