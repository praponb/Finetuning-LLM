from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-04-23")
    .groupBy(
        F.when(F.col("page").isin(["/search", "/checkout"]), "priority").otherwise("other").alias("page_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0))) / F.count("*")), 1).alias("pct_purchase"),
    )
    .filter(F.col("pct_purchase") > 58)
    .orderBy(F.col("page_group").desc())
)
