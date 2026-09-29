from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy(F.col("status_code").desc(), "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy("status_code", "id", "attr_key").rowsBetween(-6, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .select(
        "*",
        F.lag("status_code", 2).over(w).alias("prev_status_code"),
        F.ntile(10).over(w2).alias("status_code_bucket"),
        F.avg("status_code").over(w3).alias("status_code_avg_2"),
        F.avg("status_code").over(w4).alias("status_code_avg_6"),
    )
)
