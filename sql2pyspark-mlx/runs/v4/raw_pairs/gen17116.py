from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host").orderBy(F.col("latency_ms").desc_nulls_first())

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "level",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    exploded
    .withColumn("status_code_dense_rank", F.dense_rank().over(w_grp))
    .withColumn("latency_ms_cume_dist", F.cume_dist().over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w_grp))
)
