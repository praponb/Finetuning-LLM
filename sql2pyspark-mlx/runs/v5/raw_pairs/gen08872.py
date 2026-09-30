from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .cube("customer_id")
    .agg(
        F.round(F.avg("new_group_flag"), 2).alias("avg_new_group_flag"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.sum(F.when(F.col("device") == "tablet", F.col("new_group_flag"))).alias("tablet_total_new_group_flag"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("avg_new_group_flag") < 1000000)
)
