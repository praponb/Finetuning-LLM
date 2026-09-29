from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_web_events
    .withColumn("is_new_session", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy("island_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.sum(F.when(F.col("page") == "/home", F.col("is_new_session")).otherwise(0)).alias("home_is_new_session"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("purchase_rows"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
    )
    .filter(F.col("purchase_rows") > 1)
    .orderBy(F.col("avg_island_id").desc_nulls_first())
)
