from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("latency_ms").desc())
w2 = Window.partitionBy("attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "latency_ms",
        F.explode_outer("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("latency_ms_percent_rank", F.percent_rank().over(w))
    .withColumn("attr_key_sum_latency_ms", F.sum("latency_ms").over(w2))
    .withColumn("attr_key_sum_latency_ms_2", F.sum("latency_ms").over(w2))
    .withColumn("attr_key_count_latency_ms", F.count("latency_ms").over(w2))
    .withColumn("attr_key_count_latency_ms_2", F.count("latency_ms").over(w2))
)
