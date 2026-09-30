from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("device") == "tablet")
    .withColumn("started_at_date", F.to_date("started_at"))
    .withColumn("resolved_browser", F.coalesce("browser", F.lit("NONE")))
)

prev_events = base.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

result = (
    sessions_sessions
    .groupBy("device")
    .agg(
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("new_group_flag_per_uk"),
        F.min("started_at").alias("started_at"),
    )
    .orderBy(F.col("new_group_flag_per_uk").desc())
)
