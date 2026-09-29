from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("temp_c", "id")

enriched = (
    df_device_readings
    .withColumn("first_known_pressure", F.first("pressure", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    enriched
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        "seq_no",
    )
    .orderBy(F.col("humidity").asc_nulls_first(), "id")
)
