from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "id",
        "event_type",
        "event_time",
        "duration_sec",
        F.trim("page").alias("clean_page"),
        F.when(F.col("page") == "/search", 1).otherwise(0).alias("is_search"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

prev_events = valid.withColumn("prev_event_time", F.lag("event_time").over(w))

gaps = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 8, "HIGH").otherwise("LOW").alias("burst_id_tier"),
        "event_type",
        F.when(F.col("is_search") >= 80, "PREMIUM").otherwise("STANDARD").alias("is_search_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("prev_event_time").alias("first_seen"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.count_if(F.col("event_type") == "purchase").alias("purchase_events"),
        F.count("*").alias("row_count"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("duration_min"),
    )
    .filter(F.col("purchase_events") < 1)
    .orderBy("is_search_tier")
)
