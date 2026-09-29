from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("session_start_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    islands
    .groupBy("page", F.date_trunc("month", "event_time").alias("activity_month"))
    .agg(
        F.count_if(F.col("page") == "/checkout").alias("checkout_events"),
        F.percentile_approx("duration_sec", 0.9).alias("duration_sec_p90"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
    )
)
