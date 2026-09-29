from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "key").orderBy(F.col("latency_ms").desc_nulls_first(), "id", "key")
w2 = Window.partitionBy("service", "host").orderBy(F.col("latency_ms").asc_nulls_first(), "id", "key")
w3 = Window.partitionBy("service").orderBy("row_num_2", "id", "key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "latency_ms",
        F.explode("context").alias("key", "attr_value"),
    )
)

deduped = (
    lines
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_app_logs = (
    deduped
    .withColumn("row_num_2", F.row_number().over(w2))
    .filter(F.col("row_num_2") == 1)
)

latest = (
    deduped_app_logs
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    latest
    .select("id", "service", "host", "latency_ms", "key", "attr_value", "row_num_2")
    .orderBy(F.col("service").desc_nulls_last())
)
