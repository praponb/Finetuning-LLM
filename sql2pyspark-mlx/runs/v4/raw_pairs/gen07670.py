from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "k").orderBy(F.col("status_code").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("status_code", "id", "k").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("host", "k").orderBy("status_code_avg_3", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("k")

lines = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("k", "v"),
    )
)

enriched = (
    lines
    .select(
        "*",
        F.ntile(5).over(w).alias("status_code_bucket"),
        F.avg("status_code").over(w2).alias("status_code_avg_3"),
    )
)

scored = (
    enriched
    .withColumn("cumulative_status_code", F.sum("status_code").over(w3))
    .withColumn("k_max_status_code_avg_3", F.max("status_code_avg_3").over(w4))
)

result = (
    scored
    .withColumn("activity_day", F.date_trunc("day", "log_ts"))
    .groupingSets(
        [["activity_day", "host"], ["activity_day"], []],
        "activity_day",
        "host",
    )
    .agg(
        F.max("log_ts").alias("last_log_ts"),
        F.size(F.collect_set("level")).alias("num_levels"),
        F.avg(F.when(F.col("level") == "ERROR", F.col("status_code"))).alias("error_avg_status_code"),
        F.count(F.when(F.col("k") == "region", 1)).alias("region_count"),
        F.max("log_ts").alias("last_seen"),
        F.count(F.when(F.col("k") == "trace", 1)).alias("trace_rows"),
        F.grouping("activity_day").alias("grouping_level"),
    )
)
