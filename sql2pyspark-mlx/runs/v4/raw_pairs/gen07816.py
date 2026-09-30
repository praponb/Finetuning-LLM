from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_web_events
    .withColumn("island_id_tier", F.when(F.col("island_id") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "event_time"))
    .groupingSets(
        [["island_id_tier", "year_start"], ["island_id_tier"]],
        "island_id_tier",
        "year_start",
    )
    .agg(
        F.percentile_approx("is_new_burst", 0.9).alias("is_new_burst_p90"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
        F.percentile_approx("duration_sec", 0.95).alias("duration_sec_p95"),
    )
)
