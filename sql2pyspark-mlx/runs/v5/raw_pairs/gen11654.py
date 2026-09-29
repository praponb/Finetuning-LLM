from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "level", F.explode("context").alias("key", "value"))
)

result = (
    lines
    .groupBy("service")
    .agg(
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_count"),
        F.count_if(F.col("level") == "ERROR").alias("error_events"),
        F.count_if(F.col("level") == "ERROR").alias("error_events_2"),
    )
    .orderBy(F.col("error_events").desc())
)
