from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("page_length", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy(F.col("last4_count_duration_sec").desc_nulls_first(), "id")

base = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "purchase"]) & (F.col("event_time") > F.lit("2023-09-15").cast("date")))
    .withColumn("page_length", F.length("page"))
    .withColumn("event_type_lower", F.lower("event_type"))
)

with_windows = (
    base
    .select(
        "id",
        "event_type",
        "duration_sec",
        "event_type_lower",
        F.max("duration_sec").over(w).alias("last4_max_duration_sec"),
        F.min("duration_sec").over(w).alias("last4_min_duration_sec"),
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.count("duration_sec").over(w).alias("last4_count_duration_sec"),
    )
)

base_web_events = (
    with_windows
    .filter(F.col("last4_avg_duration_sec").between(320, 370))
    .select(
        "id",
        "event_type",
        "duration_sec",
        "last4_max_duration_sec",
        "last4_min_duration_sec",
        "last4_avg_duration_sec",
        "last4_count_duration_sec",
        F.col("last4_min_duration_sec").cast("int").alias("last4_min_duration_sec_int"),
    )
)

valid = (
    base_web_events
    .filter(F.col("last4_min_duration_sec_int") == 57)
    .withColumn("last4_count_duration_sec_minus_last4_max_duration_sec", (F.col("last4_count_duration_sec") - F.col("last4_max_duration_sec")))
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "last4_count_duration_sec", "last4_min_duration_sec_int")
)

result = (
    latest
    .groupBy(
        F.when(F.col("last4_min_duration_sec_int") >= 89, "PREMIUM").otherwise("STANDARD").alias("last4_min_duration_sec_int_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("last4_min_duration_sec_int"), 2).alias("avg_last4_min_duration_sec_int"),
        F.round(F.avg("last4_min_duration_sec_int"), 2).alias("avg_last4_min_duration_sec_int_2"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
