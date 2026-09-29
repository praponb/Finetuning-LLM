from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w2))

result = (
    islands
    .groupBy(
        F.when(F.col("burst_id") >= 8, "LARGE").otherwise("SMALL").alias("burst_id_tier"),
        F.date_trunc("QUARTER", "started_at").alias("quarter_start"),
        F.date_trunc("MONTH", "started_at").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
)
