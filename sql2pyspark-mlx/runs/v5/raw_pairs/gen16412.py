from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("is_new_session").desc(), "id")
w4 = Window.orderBy("pressure", "id")
w5 = Window.orderBy(F.col("prev_reading_ts").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w6 = Window.orderBy(F.col("prev_reading_ts").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.orderBy("prev_reading_ts", "id")
w8 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("next_is_new_burst_count_30d", "id")

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

with_prev = islands.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

gaps_2 = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps_2.withColumn("island_id", F.sum("is_new_session").over(w2))

ranked = (
    sessions_device_readings
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "pressure",
        "is_new_burst",
        "burst_id",
        "prev_reading_ts",
        "is_new_session",
        "island_id",
    )
)

enriched = (
    ranked
    .select(
        "id",
        "prev_reading_ts",
        "island_id",
        F.lag("pressure").over(w4).alias("prev_pressure"),
        F.lead("pressure").over(w4).alias("next_pressure"),
        F.lead("is_new_burst").over(w4).alias("next_is_new_burst"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

enriched_device_readings = (
    enriched
    .select(
        "id",
        "seq_no",
        F.max("prev_pressure").over(w5).alias("prev_pressure_max_90d"),
        F.sum("seq_no").over(w6).alias("seq_no_sum_30d"),
        F.lag("seq_no", 1).over(w7).alias("prev_seq_no"),
        F.last("prev_pressure", ignorenulls=True).over(w8).alias("last_known_prev_pressure"),
        F.count("next_is_new_burst").over(w6).alias("next_is_new_burst_count_30d"),
    )
)

metrics = (
    enriched_device_readings
    .filter(F.col("prev_pressure_max_90d") < 1000)
    .select(
        "id",
        "seq_no",
        "prev_pressure_max_90d",
        "prev_seq_no",
        "next_is_new_burst_count_30d",
        F.lead("last_known_prev_pressure").over(w9).alias("next_last_known_prev_pressure"),
        F.lag("last_known_prev_pressure").over(w9).alias("prev_last_known_prev_pressure"),
        F.lead("next_is_new_burst_count_30d").over(w9).alias("next_next_is_new_burst_count_30d"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

base = (
    metrics
    .filter(F.col("prev_last_known_prev_pressure") > 1004)
    .withColumn("prev_pressure_max_90d_whole", F.floor("prev_pressure_max_90d"))
)

result = (
    base
    .select(
        "id",
        "seq_no",
        "prev_seq_no",
        "next_last_known_prev_pressure",
        "prev_last_known_prev_pressure",
        "next_next_is_new_burst_count_30d",
        "prev_pressure_max_90d_whole",
    )
    .orderBy(F.col("prev_last_known_prev_pressure").desc())
)
