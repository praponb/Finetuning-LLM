from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("page")
w3 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w4 = Window.partitionBy("event_type").orderBy("next_duration_sec", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("event_type")
w6 = Window.partitionBy("event_type").orderBy("last_known_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_type_stddev_samp_last4_max_last_known_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("event_type").orderBy("event_type_max_last4_max_last_known_duration_sec", "id")

enriched = (
    df_web_events
    .select(
        "id",
        "event_type",
        F.last("duration_sec", ignorenulls=True).over(w).alias("last_known_duration_sec"),
        F.stddev_samp("duration_sec").over(w2).alias("page_stddev_samp_duration_sec"),
        F.lead("duration_sec", 1).over(w3).alias("next_duration_sec"),
    )
)

scored = (
    enriched
    .withColumn("last4_max_last_known_duration_sec", F.max("last_known_duration_sec").over(w4))
    .withColumn("last4_avg_last_known_duration_sec", F.avg("last_known_duration_sec").over(w4))
    .withColumn("last4_min_last_known_duration_sec", F.min("last_known_duration_sec").over(w4))
)

valid = (
    scored
    .filter(F.col("last4_max_last_known_duration_sec") <= 300)
    .select(
        "id",
        "event_type",
        "last_known_duration_sec",
        "page_stddev_samp_duration_sec",
        "next_duration_sec",
        "last4_max_last_known_duration_sec",
        "last4_min_last_known_duration_sec",
        (F.col("last4_max_last_known_duration_sec") - F.col("last4_avg_last_known_duration_sec")).alias("last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec"),
        F.when(F.col("event_type").isin(["search", "purchase"]), "Group A").otherwise("Other").alias("event_type_group"),
    )
)

with_windows = (
    valid
    .withColumn("event_type_stddev_samp_last4_max_last_known_duration_sec", F.stddev_samp("last4_max_last_known_duration_sec").over(w5))
    .withColumn("event_type_max_last4_max_last_known_duration_sec", F.max("last4_max_last_known_duration_sec").over(w5))
)

enriched_web_events = (
    with_windows
    .filter(F.col("page_stddev_samp_duration_sec") != 376)
    .withColumn("first_known_last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec", F.first("last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec", ignorenulls=True).over(w6))
    .withColumn("first_known_event_type_stddev_samp_last4_max_last_known_duration_sec", F.first("event_type_stddev_samp_last4_max_last_known_duration_sec", ignorenulls=True).over(w7))
    .withColumn("event_type_max_last4_max_last_known_duration_sec_bucket", F.ntile(4).over(w8))
)

valid_web_events = (
    enriched_web_events
    .filter(F.col("last4_max_last_known_duration_sec") < 230)
    .select(
        "*",
        (F.col("event_type_stddev_samp_last4_max_last_known_duration_sec") + F.col("event_type_max_last4_max_last_known_duration_sec")).alias("event_type_stddev_samp_last4_max_last_known_duration_sec_plus_event_type_max_last4_max_last_known_duration_sec"),
        (F.col("last4_max_last_known_duration_sec") + F.col("first_known_event_type_stddev_samp_last4_max_last_known_duration_sec")).alias("last4_max_last_known_duration_sec_plus_first_known_event_type_stddev_samp_last4_max_last_known_duration_sec"),
    )
)

result = (
    valid_web_events
    .select(
        "id",
        "event_type",
        "last_known_duration_sec",
        "page_stddev_samp_duration_sec",
        "next_duration_sec",
        "last4_min_last_known_duration_sec",
        "event_type_stddev_samp_last4_max_last_known_duration_sec",
        "first_known_last4_max_last_known_duration_sec_minus_last4_avg_last_known_duration_sec",
        "first_known_event_type_stddev_samp_last4_max_last_known_duration_sec",
        F.when(F.col("event_type").rlike("^(search|purchase)$"), "EXTENDED").when(F.col("event_type").rlike("^(page_view|click)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("event_type_zone"),
    )
)
