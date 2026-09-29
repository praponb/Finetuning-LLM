from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_web_events
    .withColumn("last_known_duration_sec", F.last("duration_sec", ignorenulls=True).over(w))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w2))
)

result = (
    with_windows
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.sort_array(F.collect_set("page")).alias("page_list_2"),
        F.min("event_time").alias("first_seen"),
        F.sum(F.when(F.col("page") == "/cart", F.col("last_known_duration_sec")).otherwise(0)).alias("cart_last_known_duration_sec"),
    )
    .filter(F.col("cart_last_known_duration_sec") < 0)
)
