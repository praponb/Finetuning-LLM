from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status_code_tier").orderBy(F.col("max_status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "latency_ms",
        "status_code",
        F.explode_outer("context").alias("k", "attr_value"),
    )
)

result = (
    lines
    .groupBy(
        F.when(F.col("status_code") >= 385, "HIGH").otherwise("LOW").alias("status_code_tier"),
        "k",
    )
    .agg(
        F.max("status_code").alias("max_status_code"),
        F.count(F.when(F.col("host") == "h1", 1)).alias("h1_rows"),
    )
    .withColumn("max_status_code_percent_rank", F.percent_rank().over(w))
)
