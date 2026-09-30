from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "attr_key").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.orderBy(F.col("status_code").desc(), "id", "attr_key")
w4 = Window.partitionBy("attr_key", "level").orderBy("status_code", "id", "attr_key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .withColumn("status_code_avg_3", F.avg("status_code").over(w))
    .withColumn("status_code_share", F.round((F.col("status_code") / F.sum("status_code").over(w2)), 4))
    .withColumn("status_code_bucket", F.ntile(5).over(w3))
    .withColumn("prev_status_code", F.lag("status_code", 1).over(w4))
)
