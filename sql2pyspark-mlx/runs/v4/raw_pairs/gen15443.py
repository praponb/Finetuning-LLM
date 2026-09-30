from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type")
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_adjusted").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec").desc(), "id")
w5 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_adjusted").asc_nulls_first(), "id")
w6 = Window.partitionBy("session_id")

filtered = (
    df_web_events
    .filter(F.col("session_id").isin([2, 8, 32]))
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.round((F.col("duration_sec") * 1.1), 2).alias("duration_sec_adjusted"),
        F.coalesce("page", F.lit("OTHER")).alias("resolved_page"),
    )
)

enriched = (
    filtered
    .withColumn("event_type_max_duration_sec", F.max("duration_sec").over(w))
    .withColumn("next_duration_sec", F.lead("duration_sec", 2).over(w2))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped_web_events = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

ranked = (
    deduped_web_events
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    ranked
    .select(
        "*",
        F.sum("duration_sec").over(w6).alias("session_id_sum_duration_sec"),
        F.min("duration_sec").over(w6).alias("session_id_min_duration_sec"),
    )
)

remaining_keys = (
    scored
    .select("session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type") == "search")
        .select("session_id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

summary = (
    kept_rows
    .groupBy(
        F.when(F.col("session_id_min_duration_sec") >= 441, "HIGH").otherwise("LOW").alias("session_id_min_duration_sec_tier"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec_adjusted")).otherwise(0)).alias("purchase_duration_sec_adjusted"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("duration_sec")).otherwise(0)).alias("purchase_duration_sec"),
        F.percentile_approx("duration_sec_adjusted", [0.25, 0.5, 0.75]).alias("duration_sec_adjusted_quartiles"),
    )
)

result = (
    summary
    .select(
        "session_id_min_duration_sec_tier",
        "purchase_duration_sec_adjusted",
        "distinct_ids",
        "purchase_duration_sec",
        "duration_sec_adjusted_quartiles",
    )
    .orderBy("purchase_duration_sec")
)
