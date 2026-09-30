from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    gaps
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        "burst_id",
        F.when(F.col("duration_sec") >= 386, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        F.round(F.avg("burst_id"), 2).alias("avg_burst_id"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.sum(F.when(F.col("page") == "/cart", F.col("burst_id"))).alias("cart_total_burst_id"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count_if(F.col("page") == "/product").alias("product_events"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
    )
    .filter(F.col("avg_burst_id") > 0)
    .orderBy(F.col("avg_duration_sec").desc())
)
