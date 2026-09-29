from pyspark.sql import functions as F

src = (
    df_web_events
    .filter(F.col("duration_sec").isNotNull() & (F.col("event_type") == "search") & F.col("page").isin(["/checkout", "/cart"]))
    .withColumn("is_purchase", F.when(F.col("event_type") == "purchase", 1).otherwise(0))
)

event_type_summary = (
    src
    .groupBy("event_type", "page")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.avg("is_purchase").alias("avg_is_purchase"),
    )
)

result = (
    event_type_summary
    .filter(F.col("avg_duration_sec") >= 350)
    .orderBy("page", "event_type")
    .limit(3)
)
