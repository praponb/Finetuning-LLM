from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

result = (
    sessions_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        "island_id",
        F.when(F.col("device").rlike("^(mobile|desktop)$"), "EXTENDED").when(F.col("device").rlike("^(tablet)$"), "PRIORITY").otherwise("CORE").alias("device_zone"),
    )
    .orderBy(F.col("browser").asc_nulls_last())
)
