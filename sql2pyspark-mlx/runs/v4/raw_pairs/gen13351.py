from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy(F.col("duration_sec_rounded").desc_nulls_last())
w2 = Window.orderBy(F.col("duration_sec").desc_nulls_last())
w3 = Window.partitionBy("event_type").orderBy(F.col("duration_sec_rank").desc())
w4 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_web_events
    .filter(F.hour("event_time").between(12, 15))
    .select("*", F.round("duration_sec", 1).alias("duration_sec_rounded"))
)

remaining_keys = (
    valid
    .select("session_id")
    .exceptAll(
        df_web_events
        .filter(F.col("page") != "/checkout")
        .select("session_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .select(
        "*",
        F.percent_rank().over(w).alias("duration_sec_rounded_percent_rank"),
        F.rank().over(w2).alias("duration_sec_rank"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.cume_dist().over(w3).alias("duration_sec_rank_cume_dist"),
        F.count("duration_sec_rounded").over(w4).alias("duration_sec_rounded_count_90d"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("duration_sec_rounded_percent_rank") >= 0.73, "PREMIUM").otherwise("STANDARD").alias("duration_sec_rounded_percent_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_count"),
        F.max("event_time").alias("last_seen"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("duration_sec_rounded_count_90d")).otherwise(0)).alias("checkout_duration_sec_rounded_count_90d"),
    )
    .filter(F.col("purchase_count") >= 1)
    .orderBy(F.col("checkout_duration_sec_rounded_count_90d").desc())
)
