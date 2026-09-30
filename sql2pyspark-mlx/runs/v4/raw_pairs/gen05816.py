from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy(F.col("status_code").desc())
w2 = Window.partitionBy("host")
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)

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
    .withColumn("status_code_rank", F.rank().over(w))
    .withColumn("host_max_status_code", F.max("status_code").over(w2))
    .withColumn("status_code_avg_2h", F.avg("status_code").over(w3))
)
