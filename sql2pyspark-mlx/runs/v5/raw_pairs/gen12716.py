from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_device_readings
    .filter(~F.col("firmware").isin(["1.0.3", "2.0.1"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        F.col("reading_ts").cast("date").alias("reading_ts_day"),
        F.date_format("reading_ts", "MMM yyyy").alias("reading_ts_label"),
    )
)

with_gaps = (
    cleaned
    .withColumn("is_new_session", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "status_bits",
        "firmware",
        "reading_ts_label",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

result = (
    islands
    .groupBy("device_code")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.sum(F.when(F.col("device_code") == "DV-02", F.col("status_bits")).otherwise(0)).alias("dv_02_status_bits"),
    )
)
