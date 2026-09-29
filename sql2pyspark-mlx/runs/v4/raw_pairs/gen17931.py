from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    sessionized
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("island_id")).otherwise(0)).alias("th_island_id"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_rows"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("span_minutes").desc())
)
