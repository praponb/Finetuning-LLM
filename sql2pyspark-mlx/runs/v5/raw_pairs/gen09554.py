from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("started_at", "id")
w2 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("country").isNotNull() & (F.col("browser").eqNullSafe(F.lit("Safari"))))
    .withColumn("is_desktop", F.when(F.col("device") == "desktop", 1).otherwise(0))
    .withColumn("started_at_hour", F.hour("started_at"))
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        "is_desktop",
        "started_at_hour",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

result = (
    sessionized
    .groupBy(
        F.when(F.col("is_desktop") >= 65, "LARGE").otherwise("SMALL").alias("is_desktop_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("started_at")) - F.unix_timestamp(F.min("started_at"))) / 60).alias("duration_min"),
        F.sum("started_at_hour").alias("total_started_at_hour"),
        F.count_if(F.col("device") == "desktop").alias("desktop_events"),
    )
    .filter(F.col("total_started_at_hour") > 0)
)
