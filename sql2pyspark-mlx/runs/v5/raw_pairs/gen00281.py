from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .groupBy(F.date_trunc("WEEK", "started_at").alias("week_start"), "island_id")
    .agg(
        F.max("started_at").alias("ended_at"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst_2"),
    )
)
