from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("adjusted_humidity").desc())
w2 = Window.orderBy(F.col("status_bits").desc(), "id")
w3 = Window.orderBy("adjusted_humidity", "id")
w4 = Window.orderBy("adjusted_humidity_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.orderBy("event_seq", "id")
w7 = Window.orderBy("reading_ts", "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 02:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "status_bits",
        F.round((F.col("humidity") * 1.15), 2).alias("adjusted_humidity"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.rank().over(w).alias("adjusted_humidity_rank"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

metrics = (
    scored
    .filter(F.col("temp_c").between(35, 36))
    .select(
        "id",
        "reading_ts",
        "event_seq",
        F.lead("temp_c", 2).over(w3).alias("next_temp_c"),
        F.sum("status_bits").over(w4).alias("cumulative_status_bits"),
        F.sum("event_seq").over(w_part).alias("event_seq_sum_15m"),
        F.lag("event_seq", 2, 0).over(w6).alias("prev_event_seq"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.lead("next_temp_c").over(w7).alias("next_next_temp_c"),
        F.lead("prev_event_seq").over(w7).alias("next_prev_event_seq"),
        F.lag("prev_event_seq").over(w7).alias("prev_prev_event_seq"),
        F.row_number().over(w7).alias("event_seq_2"),
        F.row_number().over(w6).alias("event_seq_3"),
    )
)

result = (
    enriched
    .select(
        "id",
        "reading_ts",
        "event_seq",
        "next_temp_c",
        "cumulative_status_bits",
        "event_seq_sum_15m",
        "prev_event_seq",
        "next_next_temp_c",
        "next_prev_event_seq",
    )
)
