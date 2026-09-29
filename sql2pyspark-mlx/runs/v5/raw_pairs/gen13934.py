from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("is_new_burst") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_burst_tier"),
        "event_type",
        F.when(F.col("burst_id") >= 7, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
    )
)
