from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service")
w2 = Window.orderBy("log_ts", "id", "k").rowsBetween(-5, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "value"),
    )
)

result = (
    exploded
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "host",
        "log_ts",
        "status_code",
        "k",
        "value",
        F.round((F.col("status_code") / F.sum("status_code").over(w)), 4).alias("status_code_share"),
        F.avg("status_code").over(w2).alias("status_code_avg_5"),
    )
)
