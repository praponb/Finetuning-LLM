from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w5 = Window.partitionBy("device_code").orderBy("is_new_session", "id")
w6 = Window.partitionBy("device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

gaps = (
    deduped
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("burst_id", F.sum("is_new_session").over(w3))

scored = (
    sessions_device_readings
    .select(
        "*",
        F.lag("pressure", 1).over(w4).alias("prev_pressure"),
        F.lead("burst_id", 1, 0).over(w5).alias("next_burst_id"),
        F.last("humidity", ignorenulls=True).over(w6).alias("last_known_humidity"),
    )
)

gaps_2 = (
    scored
    .withColumn("is_new_session_2", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps_2.withColumn("session_no", F.sum("is_new_session_2").over(w3))

summary = (
    islands
    .groupBy(
        F.when(F.col("is_new_session") >= 1, "PREMIUM").otherwise("STANDARD").alias("is_new_session_tier"),
        F.when(F.col("pressure") >= 1024, "PREMIUM").otherwise("STANDARD").alias("pressure_tier"),
        F.when(F.col("prev_pressure") >= 1030, "HIGH").otherwise("LOW").alias("prev_pressure_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("prev_pressure"))).alias("dv_02_total_prev_pressure"),
        F.sum(F.when(F.col("device_code") == "DV-03", F.col("is_new_session")).otherwise(0)).alias("dv_03_is_new_session"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .select(
        "is_new_session_tier",
        "pressure_tier",
        "prev_pressure_tier",
        "dv_03_count",
        "dv_02_total_prev_pressure",
        "row_count",
    )
)
