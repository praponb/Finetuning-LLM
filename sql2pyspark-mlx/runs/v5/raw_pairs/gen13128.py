from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("status_code")
w2 = Window.partitionBy("level", "key")
w3 = Window.partitionBy("level")
w4 = Window.orderBy(F.col("status_code").desc())
w5 = Window.partitionBy("host", "key").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "attr_value"),
    )
)

result = (
    flattened
    .select(
        "id",
        "host",
        "level",
        "key",
        "attr_value",
        F.cume_dist().over(w).alias("status_code_cume_dist"),
        F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4).alias("status_code_share"),
        F.round((F.col("status_code") / F.sum("status_code").over(w3)), 4).alias("status_code_share_2"),
        F.rank().over(w4).alias("status_code_rank"),
        F.avg("status_code").over(w5).alias("status_code_avg_30m"),
    )
)
