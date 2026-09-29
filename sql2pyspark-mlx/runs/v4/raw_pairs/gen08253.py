from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 510, "HIGH").otherwise("LOW").alias("duration_sec_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50"),
        F.bit_and("duration_sec").alias("duration_sec_bit_and"),
        F.count_if(F.col("event_type") == "page_view").alias("page_view_events"),
        F.round(F.sum("duration_sec") / F.nullif(F.count(F.when(F.col("page") == "/product", 1)), F.lit(0)), 2).alias("duration_sec_per_product"),
        F.percentile_approx("duration_sec", 0.5).alias("duration_sec_p50_2"),
    )
    .filter(F.col("page_view_events") > 1)
    .orderBy(F.col("duration_sec_tier").desc_nulls_first())
)
