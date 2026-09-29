from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .groupBy(
        "session_id",
        F.date_trunc("YEAR", "prev_event_time").alias("year_start"),
        "page",
    )
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.sum(F.when(F.col("page") == "/product", F.col("new_group_flag")).otherwise(0)).alias("product_new_group_flag"),
        F.max("event_time").alias("ended_at"),
    )
    .filter(F.col("product_new_group_flag") >= 0)
    .orderBy(F.col("year_start").asc_nulls_last())
)
