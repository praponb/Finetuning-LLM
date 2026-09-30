from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device_code").orderBy("humidity", "id")
w2 = Window.partitionBy("device_code").orderBy("humidity_rounded")
w3 = Window.orderBy("prev_temp_c", "id")

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.round("humidity", 2).alias("humidity_rounded"),
        F.when(F.col("pressure") >= 1026, "High").when(F.col("pressure") >= 988, "Medium").otherwise("Low").alias("pressure_band"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lag("humidity").over(w_part).alias("prev_humidity"),
        F.lag("temp_c").over(w_part).alias("prev_temp_c"),
        F.lead("temp_c").over(w_part).alias("next_temp_c"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select("id", "reading_ts", "temp_c", "humidity", "prev_temp_c")
)

scored = (
    latest
    .filter(F.hour("reading_ts").between(9, 13))
    .withColumn("next_humidity", F.lead("humidity").over(w3))
    .withColumn("next_temp_c", F.lead("temp_c").over(w3))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    scored
    .select("id", "humidity", "prev_temp_c", "next_humidity")
    .orderBy(F.col("id").desc())
)
