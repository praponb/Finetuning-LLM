from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.sum(F.when(F.col("page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
    )
    .filter(F.col("home_count") >= 33)
)
