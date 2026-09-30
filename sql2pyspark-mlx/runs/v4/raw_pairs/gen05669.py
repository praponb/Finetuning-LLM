from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .groupBy(F.date_trunc("MONTH", "started_at").alias("activity_month"))
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.round(F.sum("is_new_session") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("is_new_session_per_de"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
        F.countDistinct("browser").alias("distinct_browsers"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count("*").alias("cnt"),
    )
)
