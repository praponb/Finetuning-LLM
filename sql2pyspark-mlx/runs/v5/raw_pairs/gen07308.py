from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("duration_sec").asc_nulls_first())
w4 = Window.partitionBy("page").orderBy("duration_sec", "id")
w5 = Window.partitionBy("page").orderBy(F.col("prev_island_id").desc_nulls_last())
w6 = Window.orderBy(F.col("prev_island_id").desc_nulls_first())
w7 = Window.partitionBy("page").orderBy(F.col("prev_island_id_dense_rank").desc_nulls_last(), "id")

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

metrics = (
    sessionized
    .filter(F.hour("event_time").between(10, 14))
    .select(
        "*",
        F.percent_rank().over(w3).alias("duration_sec_percent_rank"),
        F.lead("island_id", 1).over(w4).alias("next_island_id"),
        F.lag("island_id", 1, 0).over(w4).alias("prev_island_id"),
    )
)

scored = (
    metrics
    .withColumn("next_duration_sec", F.lead("duration_sec", 1).over(w4))
    .withColumn("prev_island_id_dense_rank", F.dense_rank().over(w5))
    .withColumn("prev_island_id_dense_rank_2", F.dense_rank().over(w6))
)

cleaned = (
    scored
    .filter((F.col("event_time") >= F.lit("2023-04-19").cast("date")) & (F.col("next_duration_sec") <= 185))
    .select(
        "*",
        F.when(F.col("prev_island_id") >= 8, "High").when(F.col("prev_island_id") >= 4, "Medium").otherwise("Low").alias("prev_island_id_band"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "event_time",
        "duration_sec",
        "island_id",
        "next_island_id",
        "next_duration_sec",
        "page_filled",
    )
)

grouped = (
    ranked
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
)

result = grouped.select("year_start", "span_minutes", "duration_min")
