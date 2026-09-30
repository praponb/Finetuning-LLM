from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("island_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .cube("island_id")
    .agg(
        F.count(F.when(F.col("browser") == "Edge", 1)).alias("edge_count"),
        F.round(F.avg("island_id"), 2).alias("avg_island_id"),
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.count_if(F.col("device") == "tablet").alias("tablet_events"),
        F.round(F.avg("is_new_burst"), 2).alias("avg_is_new_burst"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.grouping_id().alias("gid"),
    )
)
