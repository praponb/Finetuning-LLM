from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("latency_ms", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key").orderBy("latency_ms", "id", "key")

flattened = df_app_logs.select("id", "latency_ms", F.explode("context").alias("key", "v"))

result = (
    flattened
    .filter(F.col("key") == "version")
    .select(
        "id",
        "key",
        F.sum("latency_ms").over(w).alias("running_sum_latency_ms"),
        F.min("latency_ms").over(w).alias("running_min_latency_ms"),
        F.max("latency_ms").over(w).alias("running_max_latency_ms"),
        F.count("latency_ms").over(w).alias("running_count_latency_ms"),
        F.lag("latency_ms", 1, 0).over(w2).alias("prev_latency_ms"),
    )
)
