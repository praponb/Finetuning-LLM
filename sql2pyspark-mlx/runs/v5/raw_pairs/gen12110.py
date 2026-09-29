from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-26 06:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.ceil("duration_sec").alias("duration_sec_whole"),
        F.concat_ws(",", "page", "event_type").alias("page_event_type_key"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

flagged = (
    valid
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(F.date_trunc("YEAR", "event_time").alias("year_start"))
    .agg(
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_count"),
        F.count_if(F.col("event_type") == "search").alias("search_events"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_rows"),
        F.min("event_time").alias("first_seen"),
        F.max(F.when(F.col("page") == "/checkout", F.col("is_new_session"))).alias("checkout_max_is_new_session"),
    )
)
