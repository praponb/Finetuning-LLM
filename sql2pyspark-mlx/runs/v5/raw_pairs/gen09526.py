from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("session_id", F.sum("is_new_burst").over(w2))

result = (
    sessions_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "session_id",
        F.when(F.col("device").rlike("^(desktop|mobile)$"), "EXTENDED").when(F.col("device").rlike("^(tablet)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("device_zone"),
    )
)
