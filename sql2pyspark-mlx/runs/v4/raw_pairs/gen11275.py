from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page", "event_type").orderBy(F.col("event_time").desc_nulls_last(), "id")

deduped = (
    df_web_events
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(
        "event_type",
        F.date_trunc("quarter", "event_time").alias("quarter_start"),
        F.when(F.col("duration_sec") >= 500, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.count_if(F.col("page") == "/cart").alias("cart_events"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.count("*").alias("event_count"),
    )
)
