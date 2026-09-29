from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("session_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .groupBy("browser", "customer_id")
    .agg(
        F.max("started_at").alias("last_seen"),
        F.sum(F.when(F.col("device") == "desktop", F.col("session_id")).otherwise(0)).alias("desktop_session_id"),
        F.count_if(F.col("browser") == "Chrome").alias("chrome_events"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_rows"),
    )
    .orderBy(F.col("desktop_session_id").asc_nulls_first())
)
