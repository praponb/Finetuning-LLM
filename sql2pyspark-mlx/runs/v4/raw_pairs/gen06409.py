from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc_nulls_last(), "id")

ranked = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .groupBy(F.date_trunc("WEEK", "event_time").alias("week_start"))
    .agg(
        F.count(F.when(F.col("event_type") == "page_view", 1)).alias("page_view_rows"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_rows"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
)
