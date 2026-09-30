from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2024-01-23").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.round("duration_sec", 1).alias("duration_sec_rounded"),
    )
)

gaps = (
    base
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .rollup(F.date_trunc("QUARTER", "event_time").alias("quarter_start"))
    .agg(
        F.avg("burst_id").alias("avg_burst_id"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("page") == "/search", 1)).alias("search_rows"),
    )
    .filter(F.col("num_rows") < 1)
    .orderBy(F.col("avg_burst_id").desc_nulls_last())
)
