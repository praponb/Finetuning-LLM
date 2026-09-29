from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service", "level")
w2 = Window.partitionBy("key").orderBy(F.col("status_code").desc())
w3 = Window.orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("level").orderBy("status_code")
w5 = Window.partitionBy("service")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode_outer("context").alias("key", "value"),
    )
)

result = (
    exploded
    .select(
        "id",
        "level",
        "key",
        "value",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.dense_rank().over(w2).alias("status_code_dense_rank"),
        F.sum("status_code").over(w3).alias("status_code_sum_15m"),
        F.percent_rank().over(w4).alias("status_code_percent_rank"),
        F.max("status_code").over(w5).alias("service_max_status_code"),
    )
)
