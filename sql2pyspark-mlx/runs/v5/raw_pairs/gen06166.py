from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    sessionized
    .groupBy(F.date_trunc("QUARTER", "started_at").alias("quarter_start"))
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.count_if(F.col("browser") == "Safari").alias("safari_events"),
        F.count("*").alias("event_count"),
    )
    .orderBy("safari_events")
)
