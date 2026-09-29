from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("humidity_times_temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("temp_c", "id")
w5 = Window.partitionBy("device_code")
w6 = Window.orderBy(F.col("first_known_temp_c").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy(F.col("humidity").desc_nulls_first())
w9 = Window.orderBy(F.col("temp_c").desc_nulls_last())

base = (
    df_device_readings
    .filter(F.col("humidity").between(41, 45))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        (F.col("humidity") * F.col("temp_c")).alias("humidity_times_temp_c"),
    )
)

flagged = (
    base
    .withColumn("session_start_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("session_start_flag").over(w2))

enriched = (
    sessionized
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w3))
    .withColumn("next_pressure", F.lead("pressure", 1).over(w4))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w5)), 4))
)

enriched_device_readings = (
    enriched
    .withColumn("seq_no", F.row_number().over(w6))
    .withColumn("running_humidity", F.sum("humidity").over(w7))
    .withColumn("humidity_dense_rank", F.dense_rank().over(w8))
    .withColumn("running_temp_c", F.sum("temp_c").over(w2))
    .withColumn("temp_c_cume_dist", F.cume_dist().over(w9))
)

remaining_keys = (
    enriched_device_readings
    .select("id")
    .intersectAll(
        df_device_readings
        .filter(F.col("humidity") <= 52)
        .select("id")
    )
)

reconciled = (
    enriched_device_readings
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("device_code", "humidity", "next_pressure", "first_known_temp_c")
    .unpivot(
        ids=["device_code"],
        values=["humidity", "next_pressure", "first_known_temp_c"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
