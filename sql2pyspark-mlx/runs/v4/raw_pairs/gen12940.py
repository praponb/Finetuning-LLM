from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("level").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("key").orderBy(F.col("latency_ms").desc())
w4 = Window.partitionBy("key")
w5 = Window.orderBy(F.col("status_code").desc())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    exploded
    .withColumn("status_code_cume_dist", F.cume_dist().over(w))
    .withColumn("latency_ms_max_30m", F.max("latency_ms").over(w2))
    .withColumn("latency_ms_rank", F.rank().over(w3))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w4)), 4))
    .withColumn("status_code_cume_dist_2", F.cume_dist().over(w5))
)
