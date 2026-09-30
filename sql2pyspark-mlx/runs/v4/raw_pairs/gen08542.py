from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc_nulls_first())
w2 = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc_nulls_first(), "id")
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter(F.col("event_time") >= F.lit("2023-09-12 15:00:00").cast("timestamp"))
    .withColumn("is_purchase", F.when(F.col("event_type") == "purchase", 1).otherwise(0))
    .withColumn("duration_sec_filled", F.coalesce("duration_sec", F.lit(0)))
)

latest = (
    base
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

latest_web_events = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

with_gaps = (
    latest_web_events
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("is_new_burst").over(w4))

result = (
    sessions_web_events
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.countDistinct("page").alias("distinct_pages"),
        F.round(F.sum("is_new_burst") / F.nullif(F.count(F.when(F.col("event_type") == "add_to_cart", 1)), F.lit(0)), 2).alias("is_new_burst_per_add_to_cart"),
        F.max(F.when(F.col("event_type") == "purchase", F.col("is_new_burst"))).alias("purchase_max_is_new_burst"),
    )
    .orderBy(F.col("distinct_pages").desc())
)
