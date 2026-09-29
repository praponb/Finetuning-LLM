from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2024-11-01")
    .groupBy(
        F.when(F.col("page").isin(["/search", "/checkout"]), "group_a").otherwise("other").alias("page_group"),
    )
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") < 64)
)
