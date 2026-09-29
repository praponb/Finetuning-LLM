from pyspark.sql import functions as F

flattened = (
    df_app_logs
    .select("id", "service", "log_ts", F.explode("context").alias("key", "v"))
)

result = (
    flattened
    .groupBy(F.date_trunc("day", "log_ts").alias("activity_day"))
    .agg(
        F.size(F.collect_set("service")).alias("num_services"),
        F.count(F.when(F.col("service") == "search", 1)).alias("search_count"),
        F.count(F.when(F.col("service") == "billing", 1)).alias("billing_rows"),
    )
    .orderBy(F.col("search_count").desc_nulls_last())
)
