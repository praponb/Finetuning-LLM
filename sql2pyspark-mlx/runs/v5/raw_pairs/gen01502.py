from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_sessions.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .withColumn("year_start", F.date_trunc("YEAR", "prev_started_at"))
    .cube("device", "year_start")
    .agg(
        F.min("prev_started_at").alias("first_prev_started_at"),
        F.min("prev_started_at").alias("first_seen"),
        F.grouping_id().alias("gid"),
    )
)
