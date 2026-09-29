from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .withColumn("is_th", F.when(F.col("country") == "TH", 1).otherwise(0))
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_sessions
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.max(F.when(F.col("country") == "SG", F.col("session_start_flag"))).alias("sg_max_session_start_flag"),
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
    )
    .filter(F.col("sg_max_session_start_flag") < 0)
)
