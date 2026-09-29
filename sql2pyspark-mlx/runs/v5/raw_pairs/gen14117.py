from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "session_id").orderBy("duration_sec", "id")
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("page")
w5 = Window.partitionBy("session_id").orderBy("duration_sec", "id")

metrics = (
    df_web_events
    .select(
        "*",
        F.ntile(10).over(w).alias("duration_sec_bucket"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
        F.first("duration_sec", ignorenulls=True).over(w3).alias("first_known_duration_sec"),
        F.max("duration_sec").over(w4).alias("page_max_duration_sec"),
        F.lag("duration_sec", 1).over(w5).alias("prev_duration_sec"),
    )
)

result = (
    metrics
    .groupingSets(
        [["session_id", "page", "event_type"], ["session_id", "page"], ["session_id"], []],
        "session_id",
        "page",
        "event_type",
    )
    .agg(
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.grouping("session_id").alias("grouping_id"),
    )
    .filter(F.col("add_to_cart_count") < 1000)
    .orderBy(F.col("session_id").desc_nulls_last())
)
