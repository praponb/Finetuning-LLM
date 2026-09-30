from pyspark.sql import functions as F

exploded = (
    df_app_logs
    .select("id", "host", "level", F.explode("context").alias("key", "attr_value"))
)

result = (
    exploded
    .groupBy("level")
    .agg(
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_rows"),
        F.sort_array(F.collect_set("host")).alias("host_list"),
        F.count_if(F.col("level") == "WARN").alias("warn_events"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
        F.count(F.when(F.col("level") == "WARN", 1)).alias("warn_rows"),
        F.size(F.collect_set("key")).alias("num_keys"),
    )
)
