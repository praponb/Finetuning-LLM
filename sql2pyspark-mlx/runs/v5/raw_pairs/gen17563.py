from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("duration_sec").desc_nulls_last())
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type", "session_id").orderBy("duration_sec_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy("event_time", "id")
w_ordered = Window.partitionBy("page").orderBy("duration_sec", "id")

base = (
    df_web_events
    .filter((~F.col("page").eqNullSafe(F.lit("/cart"))) & F.col("page").isNotNull())
    .withColumn("duration_sec_adjusted", F.round((F.col("duration_sec") * 1.1), 2))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
    .withColumn("page_clean", F.coalesce("page", F.lit("N/A")))
)

enriched = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("duration_sec_cume_dist"),
        F.last("duration_sec_adjusted", ignorenulls=True).over(w2).alias("last_known_duration_sec_adjusted"),
    )
)

metrics = (
    enriched
    .filter(F.col("duration_sec_adjusted").between(30, 55))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_adjusted",
        "duration_sec_cume_dist",
        "last_known_duration_sec_adjusted",
        F.sum("duration_sec_filled").over(w3).alias("cumulative_duration_sec_filled"),
        F.lead("duration_sec", 2, 0).over(w4).alias("next_duration_sec"),
        F.sum("duration_sec").over(w5).alias("running_duration_sec"),
        F.lead("last_known_duration_sec_adjusted", 2, 0).over(w6).alias("next_last_known_duration_sec_adjusted"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)

cleaned = (
    metrics
    .filter((F.col("event_time") >= F.to_date(F.lit("2023-06-27"))) & F.col("page").isNotNull())
    .select(
        "*",
        (F.col("duration_sec_cume_dist") + F.col("last_known_duration_sec_adjusted")).alias("duration_sec_cume_dist_plus_last_known_duration_sec_adjusted"),
        F.dayofweek("event_time").alias("event_time_dayofweek"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "duration_sec_adjusted",
        "duration_sec_cume_dist",
        "last_known_duration_sec_adjusted",
        "cumulative_duration_sec_filled",
        "next_last_known_duration_sec_adjusted",
    )
)
