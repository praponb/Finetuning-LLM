from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("checkout_count").desc(), F.col("total_duration_sec").desc())
)
