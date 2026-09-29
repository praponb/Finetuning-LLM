from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/cart", "/checkout", "/product"]))
    .groupBy(
        F.when(F.col("duration_sec") >= 404, "high").when(F.col("duration_sec") >= 72, "mid").otherwise("low").alias("duration_sec_bucket"),
    )
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_event_types"),
    )
)
