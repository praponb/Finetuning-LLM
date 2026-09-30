from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"), "device")
    .agg(
        F.sum(F.when(F.col("country") == "TH", F.col("is_new_burst")).otherwise(0)).alias("th_is_new_burst"),
        F.count("*").alias("cnt"),
        F.max("started_at").alias("ended_at"),
    )
)
