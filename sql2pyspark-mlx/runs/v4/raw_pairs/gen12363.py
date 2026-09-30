from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        "session_start_flag",
        "island_id",
        F.when(F.col("event_type").rlike("^(click|search)$"), "CORE").when(F.col("event_type").rlike("^(page_view|add_to_cart)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("event_type_zone"),
    )
)
