from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.hour("reading_ts").between(6, 13))
    .select("*", F.col("status_bits").cast("double").alias("status_bits_num"))
)

with_prev = base.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "status_bits",
        "firmware",
        "status_bits_num",
        F.sum("new_group_flag").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .withColumn("activity_day", F.date_trunc("DAY", "reading_ts"))
    .cube("activity_day", "device_code")
    .agg(
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("duration_min"),
        F.corr("temp_c", "status_bits").alias("corr_temp_c_status_bits"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
        F.grouping_id().alias("gid"),
    )
)
