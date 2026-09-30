from pyspark.sql import functions as F

result = (
    df_sessions
    .cube("browser")
    .agg(
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
        F.max("started_at").alias("last_seen"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("uk_count") > 1)
)
