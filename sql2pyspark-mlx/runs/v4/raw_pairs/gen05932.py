from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")

deduped = (
    df_web_events
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 452, "LARGE").otherwise("SMALL"))
    .cube("duration_sec_tier")
    .agg(
        F.count("*").alias("num_rows"),
        F.count_if(F.col("page") == "/home").alias("home_events"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.bit_or("duration_sec").alias("duration_sec_bit_or"),
        F.max(F.when(F.col("page") == "/cart", F.col("duration_sec"))).alias("cart_max_duration_sec"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.grouping("duration_sec_tier").alias("grouping_id"),
    )
)
