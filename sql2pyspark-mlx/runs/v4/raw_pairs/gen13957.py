from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("event_time").asc_nulls_last(), "id")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 434, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["session_id", "duration_sec_tier"], ["session_id"]],
        "session_id",
        "duration_sec_tier",
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count(F.when(F.col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        F.avg(F.when(F.col("event_type") == "add_to_cart", F.col("duration_sec"))).alias("add_to_cart_avg_duration_sec"),
        F.count_if(F.col("event_type") == "add_to_cart").alias("add_to_cart_events"),
        F.count(F.when(F.col("page") == "/product", 1)).alias("product_count"),
        F.size(F.collect_set("page")).alias("num_pages"),
    )
    .orderBy(F.col("add_to_cart_count").desc_nulls_first())
)
