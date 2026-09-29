from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(~F.col("page").isin(["/product", "/cart"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.col("event_time").cast("date").alias("event_time_day"),
    )
)

flagged = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_web_events
    .groupBy(
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("event_time").alias("last_seen"),
        F.sum(F.when(F.col("page") == "/checkout", F.col("burst_id")).otherwise(0)).alias("checkout_burst_id"),
    )
    .orderBy(F.col("burst_id_tier").desc())
)
