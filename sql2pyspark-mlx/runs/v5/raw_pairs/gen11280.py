from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("status_bits").desc())
w_part = Window.partitionBy("device_code").orderBy("temp_c_times_humidity", "id")
w3 = Window.orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_first(), "id")
w5 = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy(F.col("next_status_bits").desc_nulls_last())
w7 = Window.partitionBy("device_code").orderBy("event_seq_2", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("device_code").orderBy("prev_status_bits", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.partitionBy("device_code").orderBy(F.col("temp_c_times_humidity").desc(), "id")

valid = (
    df_device_readings
    .filter(F.col("status_bits").between(6, 10))
    .withColumn("least_pressure_status_bits", F.least("pressure", "status_bits"))
    .withColumn("temp_c_times_humidity", (F.col("temp_c") * F.col("humidity")))
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    deduped
    .select(
        "*",
        F.lag("status_bits").over(w_part).alias("prev_status_bits"),
        F.lead("status_bits").over(w_part).alias("next_status_bits"),
        F.lead("temp_c_times_humidity").over(w_part).alias("next_temp_c_times_humidity"),
        F.lag("temp_c_times_humidity").over(w_part).alias("prev_temp_c_times_humidity"),
        F.row_number().over(w_part).alias("event_seq"),
        F.first("least_pressure_status_bits", ignorenulls=True).over(w3).alias("first_known_least_pressure_status_bits"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "least_pressure_status_bits",
        "temp_c_times_humidity",
        "next_temp_c_times_humidity",
        "event_seq",
        "first_known_least_pressure_status_bits",
    )
)

with_windows_device_readings = (
    ranked
    .select(
        "id",
        "device_code",
        "status_bits",
        "temp_c_times_humidity",
        "next_temp_c_times_humidity",
        F.lag("status_bits").over(w_part).alias("prev_status_bits"),
        F.lead("status_bits").over(w_part).alias("next_status_bits"),
        F.lead("pressure").over(w_part).alias("next_pressure"),
        F.row_number().over(w_part).alias("event_seq_2"),
        F.corr("pressure", "status_bits").over(w5).alias("corr_pressure_status_bits"),
    )
)

deduped_device_readings = (
    with_windows_device_readings
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

scored = (
    deduped_device_readings
    .filter(F.col("corr_pressure_status_bits") <= 0.2)
    .select(
        "*",
        F.avg("prev_status_bits").over(w7).alias("prev_status_bits_avg_6"),
        F.avg("corr_pressure_status_bits").over(w8).alias("corr_pressure_status_bits_avg_5"),
        F.ntile(5).over(w9).alias("temp_c_times_humidity_bucket"),
    )
)

result = (
    scored
    .select(
        "id",
        "device_code",
        F.col("next_pressure").alias("NEXT_PRESSURE_VAL"),
        F.col("corr_pressure_status_bits").alias("CORR_PRESSURE_STATUS_BITS"),
        "temp_c_times_humidity",
        F.col("next_temp_c_times_humidity").alias("NEXT_TEMP_C_TIMES_HUMIDITY_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["NEXT_PRESSURE_VAL", "CORR_PRESSURE_STATUS_BITS", "temp_c_times_humidity", "NEXT_TEMP_C_TIMES_HUMIDITY_VAL"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
)
