from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

result = (
    islands
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .groupingSets(
        [["activity_day", "session_id"], ["activity_day"]],
        "activity_day",
        "session_id",
    )
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.percentile_approx("humidity", 0.95).alias("humidity_p95"),
    )
)
