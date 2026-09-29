from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page")
w4 = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_first())
w5 = Window.partitionBy("page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("first_known_duration_sec", "id")
w8 = Window.orderBy("last4_avg_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("page").orderBy("last4_avg_duration_sec", "id")
w10 = Window.partitionBy("page").orderBy(F.col("first_known_duration_sec").desc_nulls_first(), "id")
w11 = Window.partitionBy("page").orderBy("duration_sec_rank", "id")
w12 = Window.partitionBy("page").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-06-14").cast("date")) & F.col("page").isNotNull())
    .withColumn("duration_sec_level", F.when(F.col("duration_sec") >= 399, "High").when(F.col("duration_sec") >= 196, "Medium").otherwise("Low"))
)

enriched = (
    valid
    .filter(F.col("session_id") == 12)
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.count("duration_sec").over(w).alias("last4_count_duration_sec"),
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.sum("duration_sec").over(w).alias("last4_sum_duration_sec"),
    )
)

enriched_web_events = (
    enriched
    .withColumn("first_known_duration_sec", F.first("duration_sec", ignorenulls=True).over(w2))
    .withColumn("page_count_last4_avg_duration_sec", F.count("last4_avg_duration_sec").over(w3))
    .withColumn("duration_sec_rank", F.rank().over(w4))
    .withColumn("last4_avg_duration_sec_max_30d", F.max("last4_avg_duration_sec").over(w5))
    .withColumn("seq_no", F.row_number().over(w6))
)

scored = (
    enriched_web_events
    .withColumn("prev_last4_sum_duration_sec", F.lag("last4_sum_duration_sec", 1).over(w7))
    .withColumn("first_known_page_count_last4_avg_duration_sec", F.first("page_count_last4_avg_duration_sec", ignorenulls=True).over(w8))
    .withColumn("seq_no_2", F.row_number().over(w9))
)

filtered = (
    scored
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.lit("2023-04-05 05:00:00").cast("timestamp")))
    .withColumn("page_length", F.length("page"))
    .withColumn("first_known_duration_sec_times_duration_sec_rank", (F.col("first_known_duration_sec") * F.col("duration_sec_rank")))
    .withColumn("resolved_page", F.coalesce("page", F.lit("OTHER")))
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "page",
        "duration_sec",
        "last4_sum_duration_sec",
        "first_known_duration_sec",
        "duration_sec_rank",
        "seq_no",
        "first_known_page_count_last4_avg_duration_sec",
        "page_length",
    )
)

scored_web_events = (
    deduped
    .withColumn("prev_page_length", F.lag("page_length", 1).over(w7))
    .withColumn("prev_seq_no", F.lag("seq_no", 1).over(w11))
    .withColumn("last_known_page", F.last("page", ignorenulls=True).over(w12))
)

result = (
    scored_web_events
    .groupBy(
        F.when(F.col("page_length") >= 63, "LARGE").otherwise("SMALL").alias("page_length_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.sort_array(F.collect_set("page")).alias("page_list_2"),
    )
)
