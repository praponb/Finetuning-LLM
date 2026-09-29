from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("status_code").desc())

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("k", "v"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.sum("status_code").over(w).alias("status_code_sum_2h"),
        F.sum("status_code").over(w2).alias("status_code_sum_1h"),
        F.count("status_code").over(w).alias("status_code_count_2h"),
        F.dense_rank().over(w3).alias("status_code_dense_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("status_code_count_2h").between(10, 11))
    .withColumn("status_code_whole", F.floor("status_code"))
)

result = (
    filtered
    .groupBy("k")
    .pivot("service", ["billing", "checkout"])
    .agg(F.sum("status_code_sum_2h").alias("amount"), F.count(F.lit(1)).alias("cnt"))
)
