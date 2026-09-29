from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy(F.col("status_code_stddev").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("key", "v"),
    )
)

result = (
    lines
    .groupBy(
        "key",
        F.when(F.col("status_code") >= 413, "HIGH").otherwise("LOW").alias("status_code_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.stddev_samp("status_code").alias("status_code_stddev"),
    )
    .withColumn("status_code_stddev_rank", F.rank().over(w))
    .orderBy(F.col("status_code_tier").desc())
)
