from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy("status_code")
w2 = Window.partitionBy("k", "host").orderBy("latency_ms")
w3 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .withColumn("status_code_dense_rank", F.dense_rank().over(w))
    .withColumn("latency_ms_rank", F.rank().over(w2))
    .withColumn("first_known_latency_ms", F.first("latency_ms", ignorenulls=True).over(w3))
)
