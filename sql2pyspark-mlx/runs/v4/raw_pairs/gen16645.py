from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("host")
w3 = Window.partitionBy("host").orderBy(F.col("status_code").desc())

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "value"),
    )
)

result = (
    flattened
    .withColumn("status_code_max_2h", F.max("status_code").over(w))
    .withColumn("host_max_status_code", F.max("status_code").over(w2))
    .withColumn("host_sum_status_code", F.sum("status_code").over(w2))
    .withColumn("host_stddev_samp_status_code", F.stddev_samp("status_code").over(w2))
    .withColumn("status_code_cume_dist", F.cume_dist().over(w3))
)
