from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w_grp = Window.partitionBy("device_code").orderBy("temp_c", "id")
w_hist = Window.partitionBy("device_code")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_device_readings
    .filter(F.col("firmware").isin(["2.0.1", "1.0.3"]))
    .withColumn("next_status_bits", F.lead("status_bits").over(w))
    .withColumn("prev_status_bits", F.lag("status_bits").over(w))
    .withColumn("prev_pressure", F.lag("pressure").over(w))
    .withColumn("next_pressure", F.lead("pressure").over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("pressure_bucket", F.ntile(10).over(w2))
)

enriched_device_readings = (
    enriched
    .withColumn("prev_next_status_bits", F.lag("next_status_bits").over(w_grp))
    .withColumn("next_next_status_bits", F.lead("next_status_bits").over(w_grp))
    .withColumn("next_humidity", F.lead("humidity").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("corr_status_bits_humidity", F.corr("status_bits", "humidity").over(w_hist))
)

flagged = (
    enriched_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 1 HOUR")), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("is_new_session").over(w5))

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

base = (
    ranked
    .filter(F.col("is_new_session").isNotNull())
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "next_status_bits",
        "prev_status_bits",
        "prev_pressure",
        "next_pressure",
        "seq_no",
        "pressure_bucket",
        "prev_next_status_bits",
        "next_next_status_bits",
        "next_humidity",
        "corr_status_bits_humidity",
        "is_new_session",
        "session_no",
        F.least("pressure", "next_status_bits").alias("least_pressure_next_status_bits"),
        F.greatest("next_pressure", "prev_pressure").alias("greatest_next_pressure_prev_pressure"),
    )
)

result = (
    base
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "status_bits",
        "firmware",
        "next_status_bits",
        "prev_status_bits",
    )
    .orderBy(F.col("device_code").desc_nulls_last())
)
