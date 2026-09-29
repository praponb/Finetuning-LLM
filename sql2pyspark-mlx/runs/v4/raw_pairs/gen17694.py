from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("temp_c").desc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy("humidity", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("humidity")
w5 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w7 = Window.partitionBy("device_code").orderBy("temp_c")

cleaned = (
    df_device_readings
    .filter((F.col("reading_ts") > F.lit("2025-09-01").cast("date")) & (F.col("pressure") >= 1010))
    .select("*", F.lower("firmware").alias("firmware_lower"))
)

metrics = (
    cleaned
    .withColumn("prev_humidity", F.lag("humidity", 2).over(w))
    .withColumn("temp_c_rank", F.rank().over(w2))
    .withColumn("pressure_avg_3", F.avg("pressure").over(w3))
)

with_windows = (
    metrics
    .withColumn("humidity_percent_rank", F.percent_rank().over(w4))
    .withColumn("prev_pressure", F.lag("pressure", 1).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("temp_c_cume_dist", F.cume_dist().over(w7))
    .withColumn("next_pressure", F.lead("pressure", 1).over(w5))
)

result = (
    with_windows
    .select(
        "id",
        "device_code",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "firmware_lower",
        "prev_humidity",
        "temp_c_rank",
        F.when(F.col("device_code").rlike("^(DV-02|DV-03)$"), "EXTENDED").when(F.col("device_code").rlike("^(DV-01|DV-04)$"), "GROUP_B").otherwise("GROUP_A").alias("device_code_zone"),
    )
)
