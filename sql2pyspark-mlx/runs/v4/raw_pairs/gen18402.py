from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("k").orderBy(F.col("log_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("status_code").desc(), "id", "k")
w4 = Window.partitionBy("service").orderBy("status_code", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("k", "service").orderBy(F.col("status_code").desc())
w6 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("service").orderBy("log_ts", "id", "k")
w8 = Window.partitionBy("service").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

with_windows = (
    exploded
    .withColumn("last_known_k", F.last("k", ignorenulls=True).over(w))
    .withColumn("status_code_max_2h", F.max("status_code").over(w_recent))
    .withColumn("status_code_bucket", F.ntile(3).over(w3))
)

enriched = (
    with_windows
    .withColumn("last_known_status_code_max_2h", F.last("status_code_max_2h", ignorenulls=True).over(w4))
    .withColumn("status_code_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("status_code_avg_2", F.avg("status_code").over(w6))
)

with_gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("log_ts") - F.lag("log_ts").over(w7) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("is_new_burst").over(w8))

result = (
    sessionized
    .select(
        "id",
        "service",
        "log_ts",
        "status_code",
        "k",
        "last_known_k",
        "status_code_max_2h",
        "last_known_status_code_max_2h",
        "status_code_percent_rank",
    )
    .orderBy(F.col("service").desc_nulls_last())
)
