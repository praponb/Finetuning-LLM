from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w3 = Window.partitionBy("k")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("status_code_rank", F.rank().over(w_part))
    .withColumn("status_code_count_30m", F.count("status_code").over(w2))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4))
)
