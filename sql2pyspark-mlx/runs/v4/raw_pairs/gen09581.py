from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("service").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w3 = Window.partitionBy("service").orderBy("status_code")
w4 = Window.partitionBy("attr_key")

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode_outer("context").alias("attr_key", "v"),
    )
)

result = (
    lines
    .select(
        "*",
        F.count("status_code").over(w).alias("status_code_count_1h"),
        F.max("status_code").over(w2).alias("status_code_max_15m"),
        F.cume_dist().over(w3).alias("status_code_cume_dist"),
        F.stddev_samp("status_code").over(w4).alias("attr_key_stddev_samp_status_code"),
    )
)
