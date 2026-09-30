from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy("page").orderBy(F.col("island_id").desc())
w5 = Window.orderBy(F.col("event_time").desc(), "id")
w6 = Window.orderBy(F.col("island_id").desc(), "id")
w7 = Window.orderBy("island_id", "id").rowsBetween(-5, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

scored = (
    sessionized
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("duration_sec").over(w3).alias("duration_sec_sum_7d"),
        F.dense_rank().over(w4).alias("island_id_dense_rank"),
        F.row_number().over(w5).alias("seq_no"),
        F.ntile(3).over(w6).alias("island_id_bucket"),
        F.avg("duration_sec").over(w7).alias("duration_sec_avg_5"),
    )
)

cleaned = (
    scored
    .filter(F.col("event_time") > F.to_date(F.lit("2023-10-09")))
    .select("*", F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"))
)

filtered = (
    cleaned
    .filter((F.col("event_time") >= F.lit("2023-08-01 21:00:00").cast("timestamp")) & (F.col("page").eqNullSafe(F.lit("/checkout"))))
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "island_id_dense_rank",
        "island_id_bucket",
        "duration_sec_avg_5",
        "duration_sec_filled",
        F.greatest("duration_sec", "duration_sec_filled").alias("greatest_duration_sec_duration_sec_filled"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "page",
        "island_id_dense_rank",
        "island_id_bucket",
        "duration_sec_avg_5",
        "duration_sec_filled",
        "greatest_duration_sec_duration_sec_filled",
        "page_clean",
    )
    .orderBy("island_id_dense_rank", F.col("island_id_bucket").desc())
)
