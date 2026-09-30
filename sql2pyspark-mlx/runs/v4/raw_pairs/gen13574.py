from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page", "session_id").orderBy(F.col("event_time_period").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("duration_sec_sum_90d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("event_time_period").desc(), "id")
w5 = Window.partitionBy("session_id").orderBy("duration_sec_sum_90d", "id")
w6 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_first())

valid = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-04-13 18:00:00").cast("timestamp"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.date_trunc("month", "event_time").alias("event_time_period"),
    )
)

enriched = (
    valid
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w))
    .withColumn("duration_sec_sum_90d", F.sum("duration_sec").over(w2))
)

enriched_web_events = (
    enriched
    .select(
        "*",
        F.sum("duration_sec").over(w3).alias("cumulative_duration_sec"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

deduped = (
    enriched_web_events
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    deduped
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

ranked_web_events = (
    ranked
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_web_events
    .groupBy(
        F.when(F.col("seq_no") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.min("event_time_period").alias("first_event_time_period"),
        F.stddev_samp("duration_sec").alias("duration_sec_stddev"),
    )
    .orderBy(F.col("duration_sec_stddev").desc_nulls_last())
)
