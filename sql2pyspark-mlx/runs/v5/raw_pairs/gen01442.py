from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-07-19").cast("date")) & (~F.col("page").eqNullSafe(F.lit("/search"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.upper("event_type").alias("event_type_upper"),
    )
)

flagged = (
    cleaned
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("session_id", F.date_trunc("MONTH", "event_time").alias("activity_month"))
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/cart", 1)), F.lit(0)), 2).alias("duration_sec_per_cart"),
        F.count(F.when(F.col("page") == "/cart", 1)).alias("cart_rows"),
        F.max(F.when(F.col("page") == "/search", F.col("duration_sec"))).alias("search_max_duration_sec"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("cart_rows") >= 1)
    .orderBy(F.col("cart_rows").desc_nulls_first())
)
