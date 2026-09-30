from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("status_bits").desc(), "id")
w2 = Window.partitionBy("device_code")
w_grp = Window.orderBy("status_bits", "id")

with_windows = (
    df_device_readings
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("humidity_share", F.round((F.col("humidity") / F.sum("humidity").over(w2)), 4))
    .withColumn("next_status_bits", F.lead("status_bits", 2).over(w_grp))
)

result = (
    with_windows
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.size(F.collect_set("device_code")).alias("num_device_codes"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
    )
    .orderBy(F.col("span_minutes").desc())
)
