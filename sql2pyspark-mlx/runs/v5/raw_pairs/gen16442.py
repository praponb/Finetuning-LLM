from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-05-06 16:00:00"))) & F.col("page").isNotNull())
    .withColumn("event_type_prefix", F.substring("event_type", 1, 3))
)

flagged = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupingSets(
        [["session_id", "event_type"], ["session_id"]],
        "session_id",
        "event_type",
    )
    .agg(
        F.avg(F.when(F.col("event_type") == "purchase", F.col("duration_sec"))).alias("purchase_avg_duration_sec"),
        F.count(F.when(F.col("page") == "/home", 1)).alias("home_rows"),
        F.percentile_approx("session_id_2", [0.25, 0.5, 0.75]).alias("session_id_2_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("session_id").desc_nulls_first())
)
