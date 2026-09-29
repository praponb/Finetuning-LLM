from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w2 = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("cart_events_2").desc())

enriched = (
    df_web_events
    .withColumn("duration_sec_dense_rank", F.dense_rank().over(w))
    .withColumn("first_known_duration_sec", F.first("duration_sec", ignorenulls=True).over(w2))
)

result = (
    enriched
    .groupBy(
        "event_type",
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        "page",
    )
    .agg(
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.percentile_approx("duration_sec_dense_rank", [0.25, 0.5, 0.75]).alias("duration_sec_dense_rank_quartiles"),
        F.count_if(F.col("page") == "/cart").alias("cart_events_2"),
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
    )
    .filter(F.col("cart_events_2") > 1)
    .withColumn("cart_events_2_percent_rank", F.percent_rank().over(w3))
    .orderBy(F.col("event_type").asc_nulls_last())
)
