from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last())
w4 = Window.partitionBy("device_code").orderBy("island_id", "id")
w5 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessions_device_readings = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

ranked = (
    sessions_device_readings
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .filter(F.col("device_code").isin(["DV-03", "DV-02"]))
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq"),
        F.first("pressure", ignorenulls=True).over(w5).alias("first_known_pressure"),
    )
)

with_gaps = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessions_device_readings_2 = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "firmware",
        "island_id",
        "row_seq",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

result = (
    sessions_device_readings_2
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-01"])
    .agg(F.sum("session_id"))
    .select("firmware", F.col("DV-03").alias("dv_03"), F.col("DV-01").alias("dv_01"))
)
