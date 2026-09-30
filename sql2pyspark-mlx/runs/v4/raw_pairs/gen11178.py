from pyspark.sql import functions as F

lines = (
    df_app_logs
    .select("id", "service", "level", F.explode_outer("context").alias("k", "v"))
)

result = (
    lines
    .groupBy("k")
    .agg(
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_count"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.count(F.when(F.col("level") == "ERROR", 1)).alias("error_count"),
    )
    .orderBy(F.col("num_levels").desc())
)
