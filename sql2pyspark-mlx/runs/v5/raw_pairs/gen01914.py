from pyspark.sql import functions as F

exploded = df_app_logs.select("id", F.explode_outer("context").alias("attr_key", "v"))

result = (
    exploded
    .rollup("attr_key")
    .agg(
        F.count(F.when(F.col("attr_key") == "version", 1)).alias("version_count"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_count"),
        F.count_if(F.col("attr_key") == "trace").alias("trace_events"),
    )
    .filter(F.col("trace_events") >= 1)
)
