from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_device_readings
    .filter((F.col("reading_ts") >= F.to_date(F.lit("2025-09-01"))) & (F.col("firmware") == "1.0.3"))
    .select("*", F.round((F.col("pressure") * 0.9), 2).alias("pressure_adjusted"))
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

result = (
    sessions_device_readings
    .groupBy(
        F.when(F.col("session_start_flag") >= 1, "LARGE").otherwise("SMALL").alias("session_start_flag_tier"),
    )
    .agg(
        F.round(F.avg("session_start_flag"), 2).alias("avg_session_start_flag"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("status_bits"))).alias("dv_03_total_status_bits"),
        F.max("reading_ts").alias("last_seen"),
    )
    .filter(F.col("avg_session_start_flag") > 0)
    .orderBy(F.col("session_start_flag_tier").desc())
)
