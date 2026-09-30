from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") >= F.to_date(F.lit("2023-05-22")))
    .withColumn("clean_event_type", F.trim("event_type"))
    .withColumn("page_filled", F.coalesce("page", F.lit("DIRECT")))
)

flagged = (
    base
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_web_events = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        "clean_event_type",
        F.sum("is_new_burst").over(w2).alias("session_id_2"),
    )
)

result = (
    sessions_web_events
    .groupBy(
        F.date_trunc("year", "event_time").alias("year_start"),
        "page",
        F.when(F.col("session_id_2") >= 7, "PREMIUM").otherwise("STANDARD").alias("session_id_2_tier"),
    )
    .agg(
        F.countDistinct("page").alias("distinct_pages"),
        F.max("event_time").alias("last_event_time"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
    )
    .orderBy(F.col("checkout_rows").asc_nulls_last())
)
