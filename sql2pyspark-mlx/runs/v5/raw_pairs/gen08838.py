from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("is_new_burst").desc_nulls_first(), "id")

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("humidity_per_pressure", F.round((F.col("humidity") / F.col("pressure")), 2))
    .withColumn("temp_c_int", F.col("temp_c").cast("int"))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

flagged = (
    islands
    .withColumn("is_new_burst", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands_device_readings = flagged.withColumn("burst_id", F.sum("is_new_burst").over(w2))

latest = (
    islands_device_readings
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "firmware",
        "session_start_flag",
        "is_new_burst",
        "burst_id",
    )
)

result = (
    latest
    .select("id", F.col("pressure").alias("PRESSURE"), "humidity", "temp_c")
    .unpivot(
        ids=["id"],
        values=["PRESSURE", "humidity", "temp_c"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
