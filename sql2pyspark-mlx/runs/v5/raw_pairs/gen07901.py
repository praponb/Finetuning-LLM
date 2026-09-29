from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device",
        "started_at",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .select("id", "device")
    .orderBy("device")
)
