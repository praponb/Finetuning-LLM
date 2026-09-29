from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("reading_ts_hour").desc(), "id")
w2 = Window.orderBy(F.col("humidity").desc(), "id")
w_running = Window.orderBy("reading_ts_hour", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.orderBy("temp_c", "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        (F.col("humidity") - F.col("pressure")).alias("humidity_minus_pressure"),
        F.hour("reading_ts").alias("reading_ts_hour"),
    )
)

enriched = (
    valid
    .select(
        "*",
        F.row_number().over(w_grp).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.avg("humidity_minus_pressure").over(w_running).alias("humidity_minus_pressure_avg_6"),
        F.lag("humidity", 1).over(w4).alias("prev_humidity"),
    )
)

base = (
    enriched
    .filter(F.col("humidity_minus_pressure") > 49)
    .select(
        "*",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
    )
)

result = (
    base
    .rollup(F.date_trunc("DAY", "reading_ts").alias("activity_day"), "seq_no")
    .agg(
        F.percentile_approx("prev_humidity", 0.5).alias("prev_humidity_p50"),
        F.max("reading_ts").alias("last_seen"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
