from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

result = (
    sessionized
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "firmware",
        "is_new_session",
        "burst_id",
    )
    .orderBy(F.col("temp_c").desc_nulls_last(), "reading_ts")
)
