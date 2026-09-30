from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("attr_key").orderBy("latency_ms_whole", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("host").orderBy("latency_ms", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("service").orderBy(F.col("latency_ms_whole").desc_nulls_last())
w4 = Window.partitionBy("service")
w5 = Window.partitionBy("attr_key", "service")
w6 = Window.partitionBy("attr_key").orderBy("last_known_latency_ms", "id", "attr_key").rowsBetween(-6, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "latency_ms",
        F.explode("context").alias("attr_key", "v"),
    )
)

filtered = (
    lines
    .filter(F.col("attr_key").isin(["region", "version"]))
    .withColumn("service_title", F.initcap(F.trim("service")))
)

cleaned = (
    filtered
    .filter(F.col("latency_ms").between(874, 1853))
    .withColumn("latency_ms_whole", F.floor("latency_ms"))
    .withColumn("attr_key_clean", F.coalesce("attr_key", F.lit("OTHER")))
)

enriched = (
    cleaned
    .select(
        "*",
        F.last("latency_ms", ignorenulls=True).over(w_ordered).alias("last_known_latency_ms"),
        F.sum("latency_ms").over(w2).alias("cumulative_latency_ms"),
        F.dense_rank().over(w3).alias("latency_ms_whole_dense_rank"),
        F.round((F.col("latency_ms") / F.sum("latency_ms").over(w4)), 4).alias("latency_ms_share"),
    )
)

result = (
    enriched
    .select(
        "*",
        F.round((F.col("cumulative_latency_ms") / F.sum("cumulative_latency_ms").over(w5)), 4).alias("cumulative_latency_ms_share"),
        F.avg("cumulative_latency_ms").over(w6).alias("cumulative_latency_ms_avg_6"),
    )
)
