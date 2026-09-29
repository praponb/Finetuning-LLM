from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "key").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy("key", "host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("key")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

result = (
    lines
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w))
    .withColumn("status_code_count_1h", F.count("status_code").over(w2))
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w3))
    .withColumn("key_count_status_code", F.count("status_code").over(w4))
)
