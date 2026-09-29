from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)

metrics = (
    df_web_events
    .select(
        "*",
        F.min("duration_sec").over(w_part).alias("last3_min_duration_sec"),
        F.sum("duration_sec").over(w_part).alias("last3_sum_duration_sec"),
        F.count("duration_sec").over(w_part).alias("last3_count_duration_sec"),
    )
)

result = (
    metrics
    .select(
        "id",
        "event_type",
        "duration_sec",
        "last3_sum_duration_sec",
        "last3_count_duration_sec",
        F.when(F.col("event_type").rlike("^(click|search)$"), "GROUP_A").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "EXTENDED").otherwise("GROUP_B").alias("event_type_zone"),
    )
)
