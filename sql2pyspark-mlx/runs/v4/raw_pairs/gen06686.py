from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("key").orderBy("status_code", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("key", "level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w4 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("key").orderBy("session_start_flag", "id", "key")
w6 = Window.partitionBy("level").orderBy("status_code", "id", "key").rowsBetween(-6, Window.currentRow)
w7 = Window.partitionBy("key").orderBy("session_no", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)

lines = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "value"),
    )
)

metrics = (
    lines
    .select(
        "*",
        F.first("key", ignorenulls=True).over(w_grp).alias("first_known_key"),
        F.last("key", ignorenulls=True).over(w2).alias("last_known_key"),
    )
)

gaps = (
    metrics
    .withColumn("session_start_flag", F.when(F.lag("log_ts").over(w3).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w3) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("session_start_flag").over(w4))

enriched = (
    islands
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("status_code_avg_6", F.avg("status_code").over(w6))
    .withColumn("last_known_key_2", F.last("key", ignorenulls=True).over(w7))
)

result = (
    enriched
    .select(
        "id",
        "service",
        "log_ts",
        "key",
        "first_known_key",
        "last_known_key",
        "status_code_avg_6",
    )
    .orderBy(F.col("id").desc_nulls_last())
)
