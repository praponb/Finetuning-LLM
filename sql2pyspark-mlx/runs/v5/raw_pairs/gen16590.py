from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .groupBy("page")
    .agg(
        F.percentile_approx("island_id", 0.9).alias("island_id_p90"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.min("event_time").alias("started_at"),
    )
    .filter(F.col("num_pages") < 1000)
)
