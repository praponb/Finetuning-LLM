from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("rn", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("device_code")

valid = (
    df_device_readings
    .filter(F.to_date("reading_ts") < "2024-07-01")
    .withColumn("humidity_per_pressure", F.round((F.col("humidity") / F.col("pressure")), 2))
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    ranked
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

flagged = (
    sessions_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w2).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w2) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w3))

result = (
    islands
    .select(
        "*",
        F.first("humidity", ignorenulls=True).over(w4).alias("first_known_humidity"),
        F.avg("session_id").over(w5).alias("session_id_avg_6_prior"),
        F.lead("pressure", 1).over(w2).alias("next_pressure"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w6)), 4).alias("temp_c_share"),
    )
)
